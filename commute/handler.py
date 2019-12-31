import json
import pprint
class Handler(object):
    @staticmethod
    def recv(sock):
        data_bytes= sock.recv(4096)
        if not data_bytes:
            return None
        data_str = data_bytes.decode()
        try:
            data_json = json.loads(data_str)
        except Exception:
            pass
        else:
            print("%s -> %s : recv" % (
                repr(sock.getpeername()),
                repr(sock.getsockname()),
            ))
            pprint.pprint(data_json)
            return data_json

    @staticmethod
    def send(sock, data_json):
        if not data_json:
            return
        data_str = json.dumps(data_json)
        data_bytes = data_str.encode()
        sent_len = sock.send(data_bytes)
        print("%s -> %s : send" % (
            repr(sock.getsockname()),
            repr(sock.getpeername()),
        ))
        pprint.pprint(data_json)
        return sent_len

    def process(self, sock, mask):
        pass