import asyncore

from pyftpdlib import ftpserver
from ftpcloudfs.utils import smart_str

class MyDTPHandler(ftpserver.DTPHandler):
    def send(self, data):
        data=smart_str(data)
        return super(MyDTPHandler, self).send(data)

    def close(self):
        if self.file_obj is not None and not self.file_obj.closed:
            try:
                self.file_obj.close()
            except Exception, e:
                msg = "Data connection error (%s)" % e
                self.cmd_channel.log(msg)
                self.cmd_channel.respond("421 " + msg)
            finally:
                self.file_obj = None

        super(MyDTPHandler, self).close()

