import sys
import socket
from pyftpdlib.handlers import DTPHandler, FTPHandler, _strerror
from ftpcloudfs.utils import smart_str
from server import ObjectStorageAuthorizer
from multiprocessing.managers import RemoteError

class MyDTPHandler(DTPHandler):
    def send(self, data):
        data = smart_str(data)
        return DTPHandler.send(self, data)

    def close(self):
        if self.file_obj is not None and not self.file_obj.closed:
            try:
                self.file_obj.close()
            except Exception, e:
                msg = u"Data connection error (%s)" % e
                self.cmd_channel.log(msg)
                self.cmd_channel.respond(u"421 " + msg)
            finally:
                self.file_obj = None

        DTPHandler.close(self)

class MyFTPHandler(FTPHandler):
    # don't kick off client in long time transactions
    timeout = 0
    dtp_handler = MyDTPHandler
    authorizer = ObjectStorageAuthorizer()
    max_cons_per_ip = 0
    use_sendfile = False

    @staticmethod
    def abstracted_fs(root, cmd_channel):
        """Get an AbstractedFs for the user logged in on the cmd_channel."""
        cffs = cmd_channel.authorizer.get_abstracted_fs(cmd_channel.username)
        cffs.init_abstracted_fs(root, cmd_channel)
        return cffs

    def process_command(self, cmd, *args, **kwargs):
        """
        Flush the FS cache with every new FTP command (non-shared cache).

        Also track the remote ip to set the X-Forwarded-For header.
        """
        if self.fs:
            if self.fs.memcache_hosts is None:
                self.fs.flush()
            self.fs.conn.real_ip = self.remote_ip
        FTPHandler.process_command(self, cmd, *args, **kwargs)

    def ftp_MD5(self, path):
        line = self.fs.fs2ftp(path)
        try:
            md5_checksum = self.run_as_current_user(self.fs.md5, path)
        except OSError, err:
            why = _strerror(err)
            self.respond('550 %s.' % why)
        else:
            msg = md5_checksum.upper()
            self.respond('251 "%s" %s' % (line.replace('"', '""'), msg))

    def handle(self):
        """Track the ip and check max cons per ip (if needed)."""
        if self.max_cons_per_ip and self.remote_ip and self.shared_ip_map != None:
            count = 0
            try:
                self.shared_lock.acquire()
                count = self.shared_ip_map.get(self.remote_ip, 0) + 1
                self.shared_ip_map[self.remote_ip] = count
                self.logline("Connected, shared ip map: %s" % self.shared_ip_map)
            except RemoteError, e:
                self.logerror("Connection tracking failed: %s" % e)
            finally:
                self.shared_lock.release()

            self.logline("Connection track: %s -> %s" % (self.remote_ip, count))

            if count > self.max_cons_per_ip:
                self.handle_max_cons_per_ip()
                return

        FTPHandler.handle(self)

    def handle_error(self):
        """Catch some 'expected' exceptions not processed by FTPHandler/AsyncChat."""
        # this is aesthetic only
        t, v, _ = sys.exc_info()
        if t == socket.error:
            self.log("Connection error: %s" % v)
            self.handle_close()
            return

        FTPHandler.handle_error(self)

    def close(self):
        """Remove the ip from the shared map before calling close."""
        if not self._closed and self.max_cons_per_ip and self.shared_ip_map != None:
            try:
                self.shared_lock.acquire()
                if self.remote_ip in self.shared_ip_map:
                    self.shared_ip_map[self.remote_ip] -= 1
                    if self.shared_ip_map[self.remote_ip] <= 0:
                        del self.shared_ip_map[self.remote_ip]
                self.logline("Disconnected, shared ip map: %s" % self.shared_ip_map)
            except RemoteError, e:
                self.logerror("Connection tracking cleanup failed: %s" % e)
            finally:
                self.shared_lock.release()


        FTPHandler.close(self)

    # We want to log more commands.
    log_cmds_list = ["ABOR", "APPE", "DELE", "RMD", "RNFR", "RNTO", "RETR", "STOR", "MKD",]

