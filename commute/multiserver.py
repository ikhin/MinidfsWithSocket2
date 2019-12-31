
import selectors

from commute.handler import Handler
from commute.multisocket import Multisocket


class MultiServer(Multisocket):
    def __init__(self,handleclass):
        super(MultiServer, self).__init__(handleclass)
        self.local_sock=self.create_sock()

    def bind(self,address):
        self.local_sock.bind(address)

    def listen(self):
        self.local_sock.listen()
        print("listening on", self.local_sock.getsockname())
        self.sel.register(
            self.local_sock,
            selectors.EVENT_READ,
            AcceptHandler(self.sel,self.handleclass)
        )


class AcceptHandler(Handler):
    def __init__(self, sel, handleclass):
        super(AcceptHandler, self).__init__()
        self.sel = sel
        self.handleclass = handleclass

    def process(self, sock, mask):
        peer_sock, peer_adddress = sock.accept()
        print("accepted connection from", peer_adddress)
        peer_sock.setblocking(False)
        self.sel.register(
            peer_sock,
            selectors.EVENT_READ,
            data=self.handleclass()
        )