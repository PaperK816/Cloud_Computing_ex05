import socket
import threading
import time
import argparse
import sys
import os

# 确保能导入 common 和 node
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.protocol import MessageProtocol
from node.storage import Storage

class PaxosNode:
    def __init__(self, node_id: int, host: str, port: int, peers: list):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers  # list of (host, port)
        
        self.storage = Storage(node_id)
        
        # Proposer 状态
        self.current_round = 0  # 保证 proposal id 递增且唯一: (seq, node_id)
        self.lock = threading.Lock()
        
        # 运行状态
        self.running = True
        
        print(f"[Node {self.node_id}] Initialized with storage: promised={self.storage.promised_id}, accepted={self.storage.accepted_id}")

    def start(self):
        # 启动 TCP 监听线程
        server_thread = threading.Thread(target=self._server_loop, daemon=True)
        server_thread.start()
        print(f"[Node {self.node_id}] TCP server listening on {self.host}:{self.port}")

    def _server_loop(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(128)
        
        while self.running:
            try:
                conn, addr = server_socket.accept()
                threading.Thread(target=self._handle_connection, args=(conn,), daemon=True).start()
            except Exception as e:
                if not self.running:
                    break
                print(f"[Node {self.node_id}] Accept error: {e}")

    def _handle_connection(self, conn):
        try:
            with conn:
                while True:
                    msg = MessageProtocol.decode_stream(conn)
                    if not msg:
                        break
                    response = self._process_message(msg)
                    if response:
                        conn.sendall(MessageProtocol.encode(response))
        except Exception as e:
            pass

    def _process_message(self, msg: dict) -> dict:
        m_type = msg.get("type")
        with self.lock:
            if m_type == "PREPARE":
                return self._handle_prepare(msg)
            elif m_type == "ACCEPT":
                return self._handle_accept(msg)
            elif m_type == "LEARN":
                return self._handle_learn(msg)
            else:
                return {"status": "ERROR", "reason": "Unknown message type"}

    def _handle_prepare(self, msg: dict) -> dict:
        proposal_id = tuple(msg.get("proposal_id")) # [seq, node_id]
        # 比较 proposal_id: 先比 seq，再比 node_id
        if self.storage.promised_id is None or proposal_id > tuple(self.storage.promised_id):
            self.storage.update_promise(proposal_id)
            return {
                "status": "PROMISE",
                "node_id": self.node_id,
                "proposal_id": proposal_id,
                "accepted_id": self.storage.accepted_id,
                "accepted_value": self.storage.accepted_value
            }
        else:
            return {
                "status": "REJECT",
                "node_id": self.node_id,
                "promised_id": self.storage.promised_id
            }

    def _handle_accept(self, msg: dict) -> dict:
        proposal_id = tuple(msg.get("proposal_id"))
        value = msg.get("value")
        
        if self.storage.promised_id is None or proposal_id >= tuple(self.storage.promised_id):
            self.storage.update_promise(proposal_id)
            self.storage.update_accepted(proposal_id, value)
            return {
                "status": "ACCEPTED",
                "node_id": self.node_id,
                "proposal_id": proposal_id
            }
        else:
            return {
                "status": "REJECT",
                "node_id": self.node_id,
                "promised_id": self.storage.promised_id
            }

    def _handle_learn(self, msg: dict) -> dict:
        value = msg.get("value")
        proposal_id = msg.get("proposal_id")
        print(f"[Node {self.node_id}] LEARNED consensus value: {value} at proposal {proposal_id}")
        return {"status": "SUCCESS"}

    def send_rpc(self, host, port, msg, timeout=2.0):
        try:
            with socket.create_connection((host, port), timeout=timeout) as sock:
                sock.sendall(MessageProtocol.encode(msg))
                resp = MessageProtocol.decode_stream(sock)
                return resp
        except Exception as e:
            return None

    def propose(self, value) -> bool:
        \"\"\"
        作为 Proposer 发起一次 Paxos 共识过程
        \"\"\"
        with self.lock:
            self.current_round += 1
            proposal_id = [self.current_round, self.node_id]
        
        print(f"[Node {self.node_id}] Starting Paxos proposal {proposal_id} for value: {value}")
        
        # Phase 1: Prepare
        prepare_msg = {
            "type": "PREPARE",
            "proposal_id": proposal_id
        }
        
        promises = 0
        accepted_id_highest = None
        highest_v = value
        
        total_nodes = len(self.peers) + 1  # 包括自己
        quorum = total_nodes // 2 + 1
        
        # 统计自己的票
        with self.lock:
            if self.storage.promised_id is None or tuple(proposal_id) >= tuple(self.storage.promised_id):
                self.storage.update_promise(proposal_id)
                promises += 1
                if self.storage.accepted_id is not None:
                    accepted_id_highest = self.storage.accepted_id
                    highest_v = self.storage.accepted_value

        # 向其他节点并发发送 Prepare
        results = []
        threads = []
        
        def query_peer(peer):
            p_host, p_port = peer
            resp = self.send_rpc(p_host, p_port, prepare_msg)
            if resp and resp.get("status") == "PROMISE":
                results.append(resp)

        for peer in self.peers:
            t = threading.Thread(target=query_peer, args=(peer,))
            threads.append(t)
            t.start()
            
        for t in threads:
            t.join(timeout=3.0)
            
        for resp in results:
            promises += 1
            acc_id = resp.get("accepted_id")
            if acc_id is not None:
                if accepted_id_highest is None or tuple(acc_id) > tuple(accepted_id_highest):
                    accepted_id_highest = acc_id
                    highest_v = resp.get("accepted_value")
                    
        print(f"[Node {self.node_id}] Prepare phase completed. Promises: {promises}/{quorum}")
        if promises < quorum:
            print(f"[Node {self.node_id}] Prepare phase failed: quorum not reached.")
            return False
            
        # Phase 2: Accept
        proposal_value = highest_v
        accept_msg = {
            "type": "ACCEPT",
            "proposal_id": proposal_id,
            "value": proposal_value
        }
        
        accepts = 0
        with self.lock:
            if self.storage.promised_id is None or tuple(proposal_id) >= tuple(self.storage.promised_id):
                self.storage.update_promise(proposal_id)
                self.storage.update_accepted(proposal_id, proposal_value)
                accepts += 1

        accept_results = []
        accept_threads = []
        
        def send_accept(peer):
            p_host, p_port = peer
            resp = self.send_rpc(p_host, p_port, accept_msg)
            if resp and resp.get("status") == "ACCEPTED":
                accept_results.append(resp)

        for peer in self.peers:
            t = threading.Thread(target=send_accept, args=(peer,))
            accept_threads.append(t)
            t.start()
            
        for t in accept_threads:
            t.join(timeout=3.0)
            
        for resp in accept_results:
            accepts += 1
            
        print(f"[Node {self.node_id}] Accept phase completed. Accepts: {accepts}/{quorum}")
        if accepts < quorum:
            print(f"[Node {self.node_id}] Accept phase failed: quorum not reached.")
            return False
            
        # Phase 3: Learn
        learn_msg = {
            "type": "LEARN",
            "proposal_id": proposal_id,
            "value": proposal_value
        }
        print(f"[Node {self.node_id}] Consensus reached! Broadcasting LEARN for value: {proposal_value}")
        
        # 广播 Learn
        for peer in self.peers:
            threading.Thread(target=self.send_rpc, args=(peer[0], peer[1], learn_msg), daemon=True).start()
            
        # 自己也 Learn
        self._handle_learn(learn_msg)
        return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--host", type=str, default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", type=str, default="")  # format: host1:port1,host2:port2
    args = parser.parse_args()
    
    peers = []
    if args.peers:
        for p in args.peers.split(","):
            h, pt = p.split(":")
            peers.append((h, int(pt)))
            
    node = PaxosNode(args.id, args.host, args.port, peers)
    node.start()
    
    # 保持主线程运行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.running = False
        print("Node stopped.")
