#!/usr/bin/env python

import logging
import os
import random
import select
import socket
import struct
import sys
import time
import yaml


if os.environ.get("DEBUG", "")[:1].upper() in ["Y", "1"]:
    DEBUG = True
    logging.basicConfig(level=logging.DEBUG)
else:
    DEBUG = False
    logging.basicConfig()
log = logging


class TcpConnection(object):

  def __init__(self, mux, connection_id, tcp_socket, tcp_peer_addr):
    self.mux = mux
    self.connection_id = connection_id
    self.tcp_socket = tcp_socket
    self.tcp_peer_addr = tcp_peer_addr
    self.udp_peers = []
    self.tx_queue = TxQueue(connection_id)
    self.next_rx_seq = 0
    self.bytes_rx = 0
    self.bytes_tx = 0
    self.packets_rx = 0
    self.packets_tx = 0
    self.packets_retried = 0
    self.open = True

  def fileno(self):
    return self.tcp_socket.fileno()

  def push(self, data):
    self.tx_queue.push(data)

  def add_udp_peer(self, p):
    self.udp_peers.append(p)

  def when_readable(self):
    # FIXME buffer size?
    data = self.tcp_socket.recv(4096)
    if len(data)==0:
      log.info(f"TCP connection {self} closed.")
      self.close()
    self.push(data)
    self.bytes_rx += len(data)
    self.packets_rx += 1

  def close(self):
    self.tcp_socket.close()
    self.open = False

  def update_udp_peer(self, udp_socket, udp_addr):
    for udp_peer in self.udp_peers:
      if udp_peer.udp_socket==udp_socket and udp_peer.udp_addr==udp_addr:
        return
    self.udp_peers.append(UdpPeer(udp_socket, udp_addr))

class Mux(object):

  def __init__(self, config):
    self.config = config
    self.sockets = []
    self.connections = {}
    self.closed_connections = {}

  def add_socket(self, s):
    self.sockets.append(s)

  def add_connection(self, c):
    self.connections[c.connection_id] = c

  def loop(self):
    next_stats = time.time()
    while True:
      now = time.time()
      next_loop = now + 3
      # Check if it's time to show some stats.
      if now > next_stats:
        self.stats()
        next_stats = now + 1
      # First, send out pending UDP packets.
      for connection in self.connections.values():
        for packet in connection.tx_queue.packets.values():
          if now > packet.next_timestamp:
            for udp_peer in connection.udp_peers:
              udp_peer.udp_socket.udp_socket.sendto(packet.bytes, udp_peer.udp_addr)
            packet.next_timestamp += 0.1 # FIXME estimate latency
            packet.attempts += 1
          else:
            next_loop = min(next_loop, packet.next_timestamp)
      # Now, check if we have any data available.
      sockets = [ c for c in self.connections.values() if c.open ]
      sockets += self.sockets
      timeout = next_loop - now
      log.debug(f"timeout: {timeout}")
      readable_sockets, _, _ = select.select(sockets, [], [], timeout)
      log.debug(f"readable sockets: {readable_sockets}")
      for s in readable_sockets:
        s.when_readable()

  def stats(self):
    os.system("clear")
    print(f"{len(self.connections)} connections:")
    for c in self.connections.values():
      state = "OPEN" if c.open else "CLOSED"
      print(f"- id: {c.connection_id} / {state}")
      print(f"  tcp: {c.tcp_peer_addr}")
      print(f"  bytes rx/tx, packets rx/tx/retried: {c.bytes_rx}/{c.bytes_tx}, {c.packets_rx}/{c.packets_tx}/{c.packets_retried}")
      print(f"  tx_queue size: {len(c.tx_queue.packets)}")
      print(f"  next_rx_seq: {c.next_rx_seq}")

class TxPacket(object):

  def __init__(self, connection_id, seq, payload):
    self.connection_id = connection_id
    self.seq = seq
    self.payload = payload
    self.first_timestamp = time.time()
    self.next_timestamp = self.first_timestamp
    self.attempts = 0
    self.bytes = struct.pack("!LLB", connection_id, seq, 1) + payload


class TxQueue(object):

  def __init__(self, connection_id):
    self.connection_id = connection_id
    self.packets = {}
    self.next_seq = 0

  def push(self, data):
    # FIXME handle fragmentation
    self.packets[self.next_seq] = TxPacket(self.connection_id, self.next_seq, data)
    self.next_seq += 1


class TcpServer(object):

  def __init__(self, mux, tcp_config, udp_peers):
    self.mux = mux
    self.udp_peers = udp_peers
    self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.tcp_socket.bind(
      (tcp_config.get("bind-addr", "0.0.0.0"), tcp_config["bind-port"])
    )
    self.tcp_socket.listen(10)
    log.info(f"Listening for TCP connections on {tcp_config}.")
    
  def fileno(self):
    return self.tcp_socket.fileno()

  def when_readable(self):
    new_tcp_socket, new_client_addr = self.tcp_socket.accept()
    connection_id = random.randint(0, 1<<32-1)    
    log.info(f"Accepted new connection {connection_id} from {new_client_addr}.")
    c = TcpConnection(self.mux, connection_id, new_tcp_socket, new_client_addr)
    for p in self.udp_peers:
      c.add_udp_peer(p)
    # Send an empty data packet.
    # This should cause the remote end to establish the TCP client connection.
    c.push(b'')
    self.mux.add_connection(c)


class UdpPeer(object):

  def __init__(self, udp_socket, udp_addr):
    self.udp_socket = udp_socket
    self.udp_addr = udp_addr


class UdpSocket(object):

  def __init__(self, mux, udp_config, accept_new_connections):
    self.mux = mux
    self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    self.udp_socket.bind(
      (udp_config.get("bind-addr", "0.0.0.0"), udp_config.get("bind-port", 0))
    )
    self.accept_new_connections = accept_new_connections

  def fileno(self):
    return self.udp_socket.fileno()

  def when_readable(self):
    # FIXME buffersize
    data, remote_addr = self.udp_socket.recvfrom(4096)
    header_size = struct.calcsize("!LLB")
    header, payload = data[:header_size], data[header_size:]
    if len(header) < header_size:
      log.warning(f"Received short packet ({data}) on socket. Ignoring.")
      return
    connection_id, seq, packet_type = struct.unpack("!LLB", header)
    if connection_id not in self.mux.connections:
      if not self.accept_new_connections:
        log.warning(f"Received packet belonging to unknown connection ({connection_id}). Discarding.")
        return
      if packet_type!=1:
        log.info(f"Received non-data packet ({packet_type}) belonging to unknown connection ({connection_id}). Discarding.")
        return
      log.info(f"New connection detected ({connection_id}).")
      tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      connect_info = self.mux.config["tcp-client"]["tcp"]
      # FIXME nonblocking
      tcp_socket.connect((connect_info["connect-addr"], connect_info["connect-port"]))
      self.mux.add_connection(TcpConnection(self.mux, connection_id, tcp_socket, connect_info))
    connection = self.mux.connections[connection_id]
    connection.update_udp_peer(self, remote_addr)
    log.debug(f"connection={connection_id}, packet_type={packet_type}, seq={seq}, payload_size={len(payload)}")
    if packet_type==1:
      # Data packet
      if seq>connection.next_rx_seq:
        # FIXME maintain input queue
        log.warning(f"Received out-of-seq packet ({seq}, was expecting {connection.next_rx_seq}).")
        return
      if seq==connection.next_rx_seq:
        if len(payload)==0 and seq!=0:
          log.info("Got EOF on connection {connection_id}. Closing.")
          connection.close()
        else:
          # FIXME queue data instead of sending directly
          connection.tcp_socket.send(payload)
          connection.bytes_tx += len(payload)
          connection.packets_tx += 1
          connection.next_rx_seq += 1
      # Send ACK
      self.udp_socket.sendto(struct.pack("!LLB", connection_id, seq, 2), remote_addr)
    elif packet_type==2:
      # We got an ACK; remove the corresponding packet from TX queue
      packet = connection.tx_queue.packets.pop(seq, None)
      # Update the retransmission counter of the connection
      if packet:
        connection.packets_retried += packet.attempts - 1
    elif packet_type==3:
      log.info(f"Remote told us to close connection {connection_id}. Closing.")
      connection.close()
    else:
      log.warning(f"Got unknown packet type ({packet_type}). Ignoring.")



def main():
  side = sys.argv[1]
  config_file_name = sys.argv[2]
  assert side in ("tcp-client", "tcp-server")
  config = yaml.safe_load(open(config_file_name))
  mux = Mux(config)

  udp_peers = []
  for udp_config in config[side]["udp"]:
    udp_socket = UdpSocket(mux, udp_config, accept_new_connections=side=="tcp-client")
    peer_addr = udp_config.get("connect-addr"), udp_config.get("connect-port")
    udp_peers.append(UdpPeer(udp_socket, peer_addr))
    mux.add_socket(udp_socket)
  if side=="tcp-server":
    mux.add_socket(TcpServer(mux, config["tcp-server"]["tcp"], udp_peers))

  mux.loop()

if __name__=="__main__":
  main()