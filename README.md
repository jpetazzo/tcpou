


Packet format

- connection ID (4 bytes)
- seq (4 bytes)
- packet type
  1 = data packet or new connection
  2 = ack
  3 = invalid connection
  4 = ping
- payload (can only be present for data packets)

When there is a data packet with an empty payload:
- if seq=0, it means it's the first packet (indicates connection open)
- if seq!=0, it means it's the last packet (means that the connection should be closed)
