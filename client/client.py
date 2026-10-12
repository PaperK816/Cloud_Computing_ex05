import socket
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.protocol import MessageProtocol

def main():
    if len(sys.argv) < 3:
        print("Usage: python client/client.py <node_host> <node_port> <value_to_propose>")
        sys.exit(1)
        
    host = sys.argv[1]
    port = int(sys.argv[2])
    value = sys.argv[3]
    
    print(f"[Client] Connecting to Paxos node at {host}:{port} to propose: {value}")
    
    # 我们可以直接连上去发送自定义提案指令，或者在 server 中扩展 CLIENT_PROPOSE 报文
    # 为简单起见，我们在 server 监听时，如果是来自客户端的 PROPOSE 请求，则触发 node.propose(value)
    
    msg = {
        "type": "CLIENT_PROPOSE",
        "value": value
    }
    
    try:
        with socket.create_connection((host, port), timeout=3.0) as sock:
            sock.sendall(MessageProtocol.encode(msg))
            resp = MessageProtocol.decode_stream(sock)
            print(f"[Client] Response from node: {resp}")
    except Exception as e:
        print(f"[Client] Error connecting to node: {e}")

if __name__ == "__main__":
    main()
