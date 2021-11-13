local_resource(
  "tcp-client",
  serve_cmd="DEBUG=1 python -u tcpou.py tcp-client highfive.yaml",
  deps=["tcpou.py", "highfive.yaml"],
)

local_resource(
  "tcp-server",
  serve_cmd="DEBUG=1 python -u tcpou.py tcp-server highfive.yaml",
  deps=["tcpou.py", "highfive.yaml"],
)

local_resource(
  "curl-test",
  cmd="curl -m 5 http://localhost:1935",
  deps=["tcpou.py"],
)