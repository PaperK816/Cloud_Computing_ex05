import json
import struct

class MessageProtocol:
    \"\"\"
    自定义 TCP 报文协议：
    使用 4 字节大端长度前缀 + JSON 负载，保证 TCP 粘包/拆包处理正确。
    \"\"\"
    
    @staticmethod
    def encode(msg_dict: dict) -> bytes:
        data = json.dumps(msg_dict, ensure_ascii=False).encode('utf-8')
        length = len(data)
        return struct.pack('!I', length) + data

    @staticmethod
    def decode_stream(reader):
        \"\"\"
        从流中读取一个完整报文
        \"\"\"
        header = reader.read(4)
        if not header or len(header) < 4:
            return None
        length = struct.unpack('!I', header)[0]
        data = reader.read(length)
        if not data or len(data) < length:
            return None
        return json.loads(data.decode('utf-8'))
