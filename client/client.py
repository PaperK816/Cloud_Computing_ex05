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
    
    msg = {
        "type": "CLIENT_PROPOSE",
        "value": value
    }
    
    try:
        with socket.create_connection((host, port), timeout=10.0) as sock:
            sock.settimeout(10.0)
            sock.sendall(MessageProtocol.encode(msg))
            resp = MessageProtocol.decode_stream(sock)
            print(f"[Client] Response from node: {resp}")
    except socket.timeout:
        print(f"[Client] Error: Connection timed out.")
    except Exception as e:
        print(f"[Client] Error connecting to node: {e}")

if __name__ == "__main__":
    main()
