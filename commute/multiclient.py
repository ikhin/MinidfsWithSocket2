import selectors

from commute.multisocket import Multisocket
from core.parser import Parser


class MultiClient(Multisocket):

    def __init__(self,handleclass,bind_local_address=None):
        super(MultiClient, self).__init__(handleclass)
        self.bind_local_address=bind_local_address
        self.peer_address_list = []
        self.recv_count = 0

    def connect(self, remote_address):
        sock = self.create_sock()
        sock.bind(self.bind_local_address) if self.bind_local_address else None
        sock.connect_ex(remote_address)
        self.sel.register(
            sock,
            selectors.EVENT_WRITE|selectors.EVENT_READ,
            data=self.handleclass()
        )
        self.peer_address_list.append(remote_address)
        self.recv_count+=1
        print("connected to %s"%repr(remote_address))

    def operate_event(self, key, mask):
        sock = key.fileobj
        handle = key.data
        if mask & selectors.EVENT_WRITE:
            handle.send_to_one(sock,self.request)
        if mask & selectors.EVENT_READ:
            if handle.recv_from_one(sock):
                self.recv_count+=1

    def input_cmd(self):
        if self.recv_count < len(self.peer_address_list):
            return
        parser = Parser()
        cmd_str = input("MiniDFS> ")
        cmd_str = "put ptb.wrd"
        parser.judge_cmd(cmd_str)
        self.request = parser.data
        self.recv_count = 0

    def run(self):
        try:
            while True:
                self.input_cmd()
                events = self.sel.select()
                for key, mask in events:
                    self.operate_event(key,mask)
        except KeyboardInterrupt:
            pass
        finally:
            self.close()