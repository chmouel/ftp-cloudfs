import asyncore

from pyftpdlib import ftpserver
from ftpcloudfs.utils import smart_str
from server import RackspaceCloudAuthorizer

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

class MyFTPHandler(ftpserver.FTPHandler):
    dtp_handler = MyDTPHandler
    authorizer = RackspaceCloudAuthorizer()

    @staticmethod
    def abstracted_fs(root, cmd_channel):
        '''Get an AbstractedFs for the user logged in on the cmd_channel'''
        cffs = cmd_channel.authorizer.get_abstracted_fs(cmd_channel.username)
        cffs.init_abstracted_fs(root, cmd_channel)
        return cffs

    def process_command(self, cmd, *args, **kwargs):
        '''Flush the FS cache with every new FTP command'''
        if self.fs:
            self.fs.flush()
        super(MyFTPHandler, self).process_command(cmd, *args, **kwargs)

    def ftp_MD5(self, path):
        line = self.fs.fs2ftp(path)
        try:
            md5_checksum = self.run_as_current_user(self.fs.md5, path)
        except OSError, err:
            why = ftpserver._strerror(err)
            self.respond('550 %s.' % why)
        else:
            msg = md5_checksum.upper()
            self.respond('251 "%s" %s' % (line.replace('"', '""'), msg))

    def handle_max_cons_per_ip(self):
        """Too many clients are connected from the same IP"""
        msg = "Too many connections from this IP %s" % self.addr[0]
        self.log(msg)
        self.respond('421 %s.' % msg)
        self.close_when_done()

