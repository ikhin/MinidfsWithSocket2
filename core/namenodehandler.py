import math
import os
import pickle
from random import choice
from socketserver import BaseRequestHandler

from core.common import *
from core.parser import Parser
from core.tree import FileTree

class NameNodeHandler(BaseRequestHandler):
    # '''
    # Name Server handle instructions and manage data servers
    # Client can use `ls, read, fetch` cmds.
    # '''
    def __init__(self):
        super(NameNodeHandler, self).__init__()
        self.metas = None
        self.id_chunk_map = None # file id -> chunk, eg. {0: ['0-part-0'], 1: ['1-part-0']}
        self.id_file_map = None # file id -> name, eg. {0: ('README.md', 1395), 1: ('mini_dfs.py', 14603)}
        self.chunk_server_map = None # chunk -> data servers, eg. {'0-part-0': [0, 1, 2], '1-part-0': [0, 1, 2]}
        self.last_file_id = -1 # eg. 1
        self.last_data_server_id = -1 # eg. 2
        self.tree = None # dir tree
        self.load_meta()

        self.peer_address_list = []
        self.recv_count = 0

    def load_meta(self):
        # '''load Name Node Meta Data'''
        if not os.path.isfile(NAME_NODE_META_PATH):
            self.metas = {
                'id_chunk_map': {},
                'id_file_map': {},
                'chunk_server_map': {},
                'last_file_id': -1,
                'last_data_server_id': -1,
                'tree': FileTree()
            }
        else:
            with open(NAME_NODE_META_PATH, 'rb') as f:
                self.metas = pickle.load(f)
        self.id_chunk_map = self.metas['id_chunk_map']
        self.id_file_map = self.metas['id_file_map']
        self.chunk_server_map = self.metas['chunk_server_map']
        self.last_file_id = self.metas['last_file_id']
        self.last_data_server_id = self.metas['last_data_server_id']
        self.tree = self.metas['tree']

    def update_meta(self):
        # '''update Name Node Meta Data after put'''
        with open(NAME_NODE_META_PATH, 'wb') as f:
            self.metas['last_file_id'] = self.last_file_id
            self.metas['last_data_server_id'] = self.last_data_server_id
            pickle.dump(self.metas, f)

    def handle(self):
        request_buffer=self.server.request_buffer
        if ('put' or 'put2') in request_buffer['command']:
            self.put(request_buffer)
        elif ('read' or 'read2') in request_buffer['command']:
            self.read(request_buffer)
        elif ('fetch' or 'fetch2') in request_buffer['command']:
            self.fetch(request_buffer)
        elif ('ls' or 'ls2') in request_buffer['command']:
            self.ls(request_buffer)
        elif 'mkdir' in request_buffer['command']:
            self.mkdir(request_buffer)
        response_buffer = self.request.recv(4096)
        if response_buffer['finish']==1:
            self.server.recv_count += 1

    def ls(self,data):
        # '''ls print meta data info'''
        print('total', len(self.id_file_map))
        if 'ls2' in data['command']:
            self.tree.view(self.id_file_map)
        else:
            for file_id, (file_name, file_len) in self.id_file_map.items():
                print(LS_PATTERN % (file_id, file_name, file_len))

    def put(self,data):
        # '''split input file into chunk, then sent to differenct chunks'''

        in_path = data['file']['path']

        file_name = in_path.split('/')[-1]
        self.last_file_id += 1
        # update file tree
        if 'put2' in data['command']:
            dir = data['command']['put2']['save_path']
            if dir[0] == '/': dir = dir[1:]
            if dir[-1] == '/': dir = dir[:-1]
            self.tree.insert(dir, self.last_file_id)
        else:
            self.tree.add(self.last_file_id)

        server_id = (self.last_data_server_id + 1) % NUM_REPLICATION

        file_length = os.path.getsize(in_path)
        chunks = int(math.ceil(float(file_length) / CHUNK_SIZE))

        # generate chunk, add into <id, chunk> mapping
        self.id_chunk_map[self.last_file_id] = [CHUNK_PATTERN % (self.last_file_id, i) for i in range(chunks)]
        self.id_file_map[self.last_file_id] = (file_name, file_length)

        for i, chunk in enumerate(self.id_chunk_map[self.last_file_id]):
            self.chunk_server_map[chunk] = []

            # copy to 4 data nodes
            for j in range(NUM_REPLICATION):
                assign_server = (server_id + j) % NUM_DATA_SERVER
                self.chunk_server_map[chunk].append(assign_server)

                # add chunk-server info to global variable
                size_in_chunk = CHUNK_SIZE if i < chunks - 1 else file_length % CHUNK_SIZE
                if assign_server not in data['server_chunks']:
                    data['server_chunks'][assign_server] = []
                data['server_chunks'][assign_server].append({
                    'chunk':chunk,
                    'offset': CHUNK_SIZE * i,
                    'count': size_in_chunk,
                })
            server_id = (server_id + NUM_REPLICATION) % NUM_DATA_SERVER

        self.last_data_server_id = (server_id - 1) % NUM_DATA_SERVER
        self.update_meta()
        data['file']['id'] = self.last_file_id
        self.request.send(data)

    def read(self, data):
        # '''assign read mission to each data node'''

        file_id = data['file']['id']
        read_offset = data['read']['offset']
        read_count = data['read']['count']

        # find file_id
        file_dir =data['file']['dir']
        if 'read2' in data['command']:
            file_id = self.tree.get_id_by_path(file_dir, self.id_file_map)
            data['file']['id']= file_id
            if file_id < 0:
                print('No such file:', file_dir)
                return False

        if file_id not in self.id_file_map:
            print('No such file with id =', file_id)
        elif read_offset < 0 or read_count < 0:
            print('Read offset or count cannot less than 0')
        elif (read_offset + read_count) > self.id_file_map[file_id][1]:
            print('The expected reading exceeds the file, file size:', self.id_file_map[file_id][1])
        else:
            start_chunk = int(math.floor(read_offset / CHUNK_SIZE))
            space_left_in_chunk = (start_chunk + 1) * CHUNK_SIZE - read_offset

            if space_left_in_chunk < read_count:
                print('Cannot read across chunks')
            else:
                # randomly select a data server to read chunk
                read_server_candidates = self.chunk_server_map[CHUNK_PATTERN % (file_id, start_chunk)]
                read_server_id = choice(read_server_candidates)
                data['command']['read']['chunk']= CHUNK_PATTERN % (file_id, start_chunk)
                data['command']['read']['offset'] = read_offset - start_chunk * CHUNK_SIZE

        return False

    def fetch(self,data):
        # '''assign download mission'''
        file_id = data['file']['id']

        # find file_id
        file_dir = data['file']['dir']
        if "fetch2" in data['command']:
            file_id = self.tree.get_id_by_path(file_dir, self.id_file_map)
            data['file']['id'] = file_id
            if file_id < 0:
                data['command']['fetch']['chunks'] = -1
                print('No such file:', file_dir)
                return None

        if file_id not in self.id_file_map:
            data['command']['fetch']['chunks'] = -1
            print('No such file with id =', file_id)
        else:
            file_chunks = self.id_chunk_map[file_id]
            # print(self.id_chunk_map)
            data['command']['fetch']['chunks']= len(file_chunks)
            # get file's data server
            for chunk in file_chunks:
                data['command']['fetch']['servers'].append(self.chunk_server_map[chunk][0])
            self.request.send(data)
            return True
        return None

    def mkdir(self,json_data):
        self.tree.insert(json_data['file']['dir'])
