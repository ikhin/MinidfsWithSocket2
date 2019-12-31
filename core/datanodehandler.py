import os

from commute.handler import Handler
from core.common import *

class DataNodeHandler(Handler):
    """Data Server: execute command from nameserver."""
    server_id = 0
    def __init__(self):
        super(DataNodeHandler, self).__init__()

    def process(self, sock, mask):
        request = self.recv(sock)
        if request:
            response =None
            self.server_id = "%s"%self.server_id
            if ('put' or 'put2') in request['command']:
                response = self.save(request)
            elif ('read' or 'read2') in request['command']:
                response = self.read(request)
            self.send(sock,response)

    def save(self,json_data):
        """Data Node save file"""
        data_node_dir = DATA_NODE_DIR % (self.server_id,)
        with open(json_data['file']['path'], 'r') as f_in:
            for item in json_data['server_chunks'][self.server_id]:
                chunk = item['chunk']
                count = item['count']
                offset = item['offset']
                f_in.seek(offset, 0)
                content = f_in.read(count)
                with open(data_node_dir + os.path.sep + chunk, 'w') as f_out:
                    f_out.write(content)
                    f_out.flush()
        return {'command':'save','finish':1}

    def read(self,json_data):
        """read chunk according to offset and count"""
        read_path = (DATA_NODE_DIR % (self.server_id,)) + os.path.sep + json_data['command']['read']['chunk']
        with open(read_path, 'r') as f_in:
            f_in.seek(json_data['command']['read']['offset'])
            content = f_in.read(json_data['command']['read']['count'])
            print(content)
        return {'command':'read','finish':1}