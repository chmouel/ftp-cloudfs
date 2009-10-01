import asyncore

from pyftpdlib import ftpserver

from ftpcloudfs.utils import smart_str

class MyDTPHandler(ftpserver.DTPHandler):
    def __init__(self, *args):
        ftpserver.DTPHandler.__init__(self, *args)

    def send(self, data):
        data=smart_str(data)
        result = asyncore.dispatcher.send(self, data)
        self.tot_bytes_sent += result
        return result

class MyFTPHandler(ftpserver.FTPHandler):
    def __init__(self, *args):
        ftpserver.FTPHandler.__init__(self, *args)
        self.dtp_handler = MyDTPHandler
