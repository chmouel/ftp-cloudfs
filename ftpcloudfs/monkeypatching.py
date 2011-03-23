import asyncore

from pyftpdlib import ftpserver
from ftpcloudfs.utils import smart_str

class MyDTPHandler(ftpserver.DTPHandler):
    def send(self, data):
        data=smart_str(data)
        return super(MyDTPHandler, self).send(data)
