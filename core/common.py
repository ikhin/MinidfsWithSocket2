# -*- coding: utf-8 -*-
from enum import Enum

# DFS Meta Data
CHUNK_SIZE = 2 * 1024 * 1024
NUM_REPLICATION = 4
CHUNK_PATTERN = '%s-part-%s'

# Name Node Meta Data
NAME_NODE_META_PATH = './dfs/namenode/meta.pkl'

# Data Node
DATA_NODE_DIR = './dfs/datanode%s'

LS_PATTERN = '%s\t%20s\t%10s'

# Operations
operation_names = ('put', 'read', 'fetch', 'quit', 'ls', 'put2', 'read2', 'fetch2', 'ls2', 'mkdir')
COMMAND = Enum('COMMAND', operation_names)

NAME_NODE = ('127.0.0.1', 44803)
DATA_NODE_FIRST = ('127.0.0.1', 51500)
NUM_DATA_SERVER = 2