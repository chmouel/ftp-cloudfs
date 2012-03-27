import asyncore

from pyftpdlib import ftpserver
from ftpcloudfs.utils import smart_str
from server import RackspaceCloudAuthorizer
from multiprocessing.managers import RemoteError

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
    # don't kick off client in long time transactions
    timeout = 0
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
            if not self.fs.single_cache:
                self.fs.flush()
            self.fs.connection.real_ip = self.remote_ip
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

    def handle(self):
        """Track the ip and check max cons per ip (if needed)"""
        if self.server.max_cons_per_ip and self.remote_ip and self.shared_ip_map != None:
            try:
                self.shared_lock.acquire()
                count = self.shared_ip_map.get(self.remote_ip, 0)
                self.shared_ip_map[self.remote_ip] = count + 1
                self.shared_lock.release()
            except RemoteError, e:
                self.log("connection tracking failed: %s" % e)

            self.logline("connection track: %s -> %s" % (self.remote_ip, count+1))

            if self.shared_ip_map[self.remote_ip] > self.server.max_cons_per_ip:
                self.handle_max_cons_per_ip()
                return

            self.logline("connected, shared ip map: %s" % self.shared_ip_map)

        super(MyFTPHandler, self).handle()

    def close(self):
        """Remove the ip from the shared map before calling close"""
        if not self._closed and self.server.max_cons_per_ip and self.shared_ip_map != None:
            if self.remote_ip in self.shared_ip_map:
                try:
                    self.shared_lock.acquire()
                    self.shared_ip_map[self.remote_ip] -= 1
                    if self.shared_ip_map[self.remote_ip] <= 0:
                        del self.shared_ip_map[self.remote_ip]
                    self.shared_lock.release()
                except RemoteError, e:
                    self.log("connection tracking cleanup failed: %s" % e)

                self.logline("disconnected, shared ip map: %s" % self.shared_ip_map)

        super(MyFTPHandler, self).close()

