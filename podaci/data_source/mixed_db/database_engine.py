# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 10:27:35 2017

@author: ldh
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
db_state = 'mssql+pymssql://{user}:{pws}@{server}/{db}'

server1 = '172.24.181.55'
db1 = 'wind_db'
user1 = 'fsb'
passwd1 = 'fsb'

server2 = '172.19.62.10'
db2 = 'sds209635243_db'
user2 = 'financial_xiaoyi'
passwd2 = 'fs95536!'

server_guosen  = '172.24.182.29:1937'
db_guosen = 'GuoSenIDC'
user_guosen = 'fsb'
passwd_guosen = 'fsb'

class DatabaseEngine():
    def __init__(self,type_):
        self.type_ = type_
        self.engine = None
        
    def get_engine(self):
        if self.type_ == 'wind':
            self.engine = create_engine(db_state.format(
                    user = user1,
                    pws = passwd1,
                    server = server1,
                    db = db1))
        elif self.type_ == 'local':
            self.engine = create_engine(db_state.format(
                    user = user2,
                    pws = passwd2,
                    server = server2,
                    db = db2))
        elif self.type_ == 'guosen':
            self.engine = create_engine(db_state.format(
                    user = user_guosen,
                    pws = passwd_guosen,
                    server = server_guosen,
                    db = db_guosen))            
        return self.engine
    
    def get_session(self):
        assert self.engine
        self.session = sessionmaker(bind = self.engine)()
        return self.session