import selectors
import socket
from inspect import isfunction, isclass


class Multisocket(object):
    name = ""
    def __init__(self,handleclass):
        self.sel = selectors.DefaultSelector()
        self.handleclass=handleclass

    @staticmethod
    def create_sock(block=False):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.setblocking(block)
        return sock

    def operate_event(self, key, mask):
        sock = key.fileobj
        handle = key.data
        handle.process(sock, mask)

    def run(self):
        try:
            while True:
                events = self.sel.select()
                for key, mask in events:
                    self.operate_event(key,mask)
        except KeyboardInterrupt:
            pass
        finally:
            self.close()

    def close(self):
        events = self.sel.select()
        for key, mask in events:
            key.fileobj.close()
            self.sel.unregister(key.fileobj)
        self.sel.close()


