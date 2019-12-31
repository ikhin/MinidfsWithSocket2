import os

from commute.multiclient import MultiClient
from commute.multiserver import MultiServer
from core.common import *
from core.datanodehandler import DataNodeHandler
from core.namenodehandler import NameNodeHandler


def init_data_node(name,server_id):
    DataNodeHandler.server_id=server_id
    data_node = MultiServer(DataNodeHandler)
    data_node.name="%s%s"%(name,server_id)
    data_node.bind((DATA_NODE_FIRST[0], DATA_NODE_FIRST[1] + server_id))
    data_node.listen()
    data_node.run()

def init_name_node(name):
    def make_dfs_dir():
        if not os.path.isdir("dfs"):
            os.makedirs("dfs")
            for i in range(NUM_DATA_SERVER):
                os.makedirs("dfs/datanode%d" % i)
            os.makedirs("dfs/namenode")
    make_dfs_dir()
    address_list = [(DATA_NODE_FIRST[0], DATA_NODE_FIRST[1] + i) for i in range(NUM_DATA_SERVER)]
    name_node = MultiClient(NameNodeHandler, NAME_NODE)
    name_node.name = name
    for i in address_list:
        name_node.connect(i)
    name_node.run()

