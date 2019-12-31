import os

from core.common import *

class Parser:
    def __init__(self):
        pass

    def judge_cmd(self,cmd):
        """parse command"""
        self.data = {'server_chunks': {}, 'command': {}, 'file': {}}
        cmds = cmd.split()
        flag = False
        if len(cmds) >= 1 and cmds[0] in operation_names:
            if cmds[0] == operation_names[0]:
                if len(cmds) != 2:
                    print('Usage: put source_file_path')
                else:
                    if not os.path.isfile(cmds[1]):
                        print('Error: input file does not exist')
                    else:
                        self.data['file']['path']=cmds[1]
                        self.data['command']['put']={}
                        flag = True
            elif cmds[0] == operation_names[1]:
                if len(cmds) != 4:
                    print('Usage: read file_id offset count')
                else:
                    try:
                        self.data['file']['id']=int(cmds[1])
                        self.data['command']['read']={
                            'offset':int(cmds[2]),
                            'count':int(cmds[3]),
                        }
                    except ValueError:
                        print('Error: fileid, offset, count should be integer')
                    else:
                        flag = True
            elif cmds[0] == operation_names[2]:
                if len(cmds) != 3:
                    print('Usage: fetch file_id save_path')
                else:
                    self.data['command']['fetch']={
                        'save_path':cmds[2],
                    }
                    base = os.path.split(cmds[2])[0]
                    if len(base) > 0 and not os.path.exists(base):
                        print('Error: input save_path does not exist')
                    else:
                        try:
                            self.data['file']['id'] = int(cmds[1])
                        except ValueError:
                            print('Error: fileid should be integer')
                        else:
                            flag = True
            elif cmds[0] == operation_names[3]:
                if len(cmds) != 1:
                    print('Usage: quit')
                else:
                    print("Bye: Exiting miniDFS...")
                    os._exit(0)
                    flag = True
                    self.data['command']['quit']={}
            elif cmds[0] == operation_names[4]:
                if len(cmds) != 1:
                    print('Usage: ls')
                else:
                    flag = True
                    self.data['command']['ls']={}
            elif cmds[0] == operation_names[5]:
                # put2
                if len(cmds) != 3:
                    print('Usage: put2 source_file_path put_savepath')
                else:
                    flag = True
                    self.data['command']['put2']={
                        'save_path': cmds[1],
                    }
                    self.data['file']['path']=cmds[1]
            elif cmds[0] == operation_names[6]:
                # read2
                if len(cmds) != 4:
                    print('Usage: read2 file_dir offset count')
                else:
                    self.data['file']['dir']=cmds[1]
                    try:
                        self.data['command']['read']={
                            'offset':int(cmds[2]),
                            'count':int(cmds[3]),
                        }
                    except ValueError:
                        print('Error: offset, count should be integer')
                    else:
                        flag = True
            elif cmds[0] == operation_names[7]:
                # fetch2
                if len(cmds) != 3:
                    print('Usage: fetch2 file_dir save_path')
                else:
                    self.data['file']['dir']=cmds[1]
                    self.data['command']['fetch2']={
                        'save_path':cmds[2],
                    }
                    base = os.path.split(cmds[2])[0]
                    if len(base) > 0 and not os.path.exists(base):
                        print('Error: input save_path does not exist')
                    else:
                        flag = True
            elif cmds[0] == operation_names[8]:
                # ls2
                if len(cmds) != 1:
                    print('Usage: ls2')
                else:
                    flag = True
                    self.data['command']['ls2']={}
            elif cmds[0] == operation_names[9]:
                # mkdir
                if len(cmds) != 2:
                    print('Usage: mkdir file_dir')
                else:
                    self.data['command']['mkdir']={}
                    self.data['file']['dir']=cmds[1]
                    flag = True
            else:
                pass
        else:
            print('Usage: put|read|fetch|quit|ls|put2|read2|fetch2|ls2|mkdir')
        return flag