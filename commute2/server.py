import socket
from socketserver import BaseRequestHandler, TCPServer, BaseServer

from core.common import NAME_NODE, NUM_DATA_SERVER
from core.parser import Parser


class EchoHandler(BaseRequestHandler):
    def handle(self):
        print('Got connection from', self.client_address)
        while True:
            msg = self.request.recv(8192)
            self.request.send(msg)

if __name__ == '__main__':
    from threading import Thread
    serv = TCPServer(NAME_NODE, EchoHandler,bind_and_activate=False)
    serv.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serv.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    for n in range(NUM_DATA_SERVER):
        t = Thread(target=serv.serve_forever)
        t.daemon = True
        t.start()
    serv.serve_forever()


class NamenodeServer(TCPServer):
    def __init__(self,server_address, RequestHandlerClass):
        super(NamenodeServer, self).__init__(server_address, RequestHandlerClass,False)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.peer_address_list = []
        self.recv_count = 0

    def input_cmd(self):
        if self.recv_count < len(self.peer_address_list):
            return
        parser = Parser()
        cmd_str = input("MiniDFS> ")
        cmd_str = "put ptb.wrd"
        parser.judge_cmd(cmd_str)
        self.request_buffer = parser.data
        self.recv_count = 0