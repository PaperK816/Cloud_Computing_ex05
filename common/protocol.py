import json
import struct

class MessageProtocol:
    """
    自定义 TCP 报文协议：
    使用 4 字节大端长度前缀 + JSON 负载，保证 TCP 粘包/拆包处理正确。
    兼容 socket 对象和文件流对象。
    """
    
    @staticmethod
    def encode(msg_dict: dict) -> bytes:
        data = json.dumps(msg_dict, ensure_ascii=False).encode('utf-8')
        length = len(data)
        return struct.pack('!I', length) + data

    @staticmethod
    def _read_exact(stream, n: int) -> bytes:
        """从 stream (socket 或 file object) 中精确读取 n 个字节"""
        if hasattr(stream, "recv"):
            # socket 对象
            data = b""
            while len(data) < n:
                packet = stream.recv(n - len(data))
                if not packet:
                    break
                data += packet
            return data
        elif hasattr(stream, "read"):
            # 文件流 / file-like 对象
            return stream.read(n)
        else:
            raise TypeError("Stream must be a socket or file-like object")

    @staticmethod
    def decode_stream(stream):
        header = MessageProtocol._read_exact(stream, 4)
        if not header or len(header) < 4:
            return None
        length = struct.unpack('!I', header)[0]
        data = MessageProtocol._read_exact(stream, length)
        if not data or len(data) < length:
            return None
        return json.loads(data.decode('utf-8'))
