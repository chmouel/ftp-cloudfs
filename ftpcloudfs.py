import os
import datetime
import time

from pyftpdlib import ftpserver
import cloudfiles

__version__ = '0.1'

class CloudOperations(object):
    def __init__(self):
        self.connection = None
        self.username = None
        
    def authenticate(self, username, api_key):
        self.username = username
        self.connection = cloudfiles.get_connection(username, api_key)    

operations = CloudOperations()
        
class RackspaceCloudAuthorizer(ftpserver.DummyAuthorizer):
    '''FTP server authorizer. Logs the users into Rackspace Cloud
    Files and keeps track of them.
    '''
    users = {}

    def validate_authentication(self, username, password):
        try:
            operations.authenticate(username, password)
            return True
        except(cloudfiles.errors.AuthenticationFailed):
            pass
        return False
            
    def has_user(self, username):
        #print '#### has_user', username
        return username != 'anonymous'
        
    def has_perm(self, username, perm, path=None):
        return True
        
    def get_perms(self, username):
        return 'lrdw'
        
    def get_home_dir(self, username):
        return os.sep + username
        
    def get_msg_login(self, username):
        return 'Welcome %s' % username

    def get_msg_quit(self, username):
        return 'Goodbye %s' % username

class RackspaceCloudFilesFS(ftpserver.AbstractedFS):
    '''Rackspace Cloud Files File system emulation for FTP server.
    '''
    def parse_fspath(self, path):
        '''Returns a (username, site, filename) tuple. For shorter paths
        replaces not provided values with empty strings.
        '''
        if not path.startswith(os.sep):
            raise ValueError('parse_fspath: You have to provide a full path')
        parts = path.split(os.sep)[1:]
        if len(parts) > 3:
            raise ValueError('parse_fspath: Path too deep')
        while len(parts) < 3:
            parts.append('')
        return tuple(parts)

    def chdir(self, path):
        if path.startswith(self.root):
            username, container, obj = self.parse_fspath(path)
            if container:
                try:
                    cnt = operations.connection.get_container(container)
                    self.cwd = self.fs2ftp(path)
                    return
                except(cloudfiles.errors.NoSuchContainer):
                    raise OSError(2, 'No such file or directory')
            else:
                self.cwd = self.fs2ftp(path)
                return
                
        raise OSError(2, 'No such file or directory.')
                
    def get_list_dir(self, path):
        try:
            username, container, obj = self.parse_fspath(path)
        except(ValueError):
            raise OSError(2, 'No such file or directory')

        if not container and not obj:
            attributes = {}
            containers = operations.connection.list_containers_info()
            return self.format_list_containers(containers)

        if container and not obj:
            attributes = {}
            try:
                cnt = operations.connection.get_container(container)
                objects = cnt.list_objects_info()
            except(cloudfiles.errors.NoSuchContainer):
                raise OSError(2, 'No such file or directory')
            return self.format_list_objects(objects)

    def format_list_objects(self, items):
        for name in items:
            ts = datetime.datetime(
                *time.strptime(
                    name['last_modified'][:name['last_modified'].find('.')],
                    "%Y-%m-%dT%H:%M:%S")[0:6]).strftime("%b %d %H:%M")
            
            yield '-rw-rw-rw-   1 %s   group  %8s %s %s\r\n' % \
                (operations.username, name['bytes'], ts, name['name'])
    
    def format_list_containers(self, items):
        for name in items:
            yield 'drw-rw-rw-   1 %s   group  %8s Jan 01 00:00 %s\r\n' % \
                (operations.username, name['bytes'], name['name'])
            
    def get_stat_dir(self, rawline):
        raise OSError(40, 'unsupported')
        
    def format_mlsx(self, basedir, listing, perms, facts, ignore_err=True):
        raise OSError(40, 'unsupported')

            
def main():
    bind = 2021
    
    ftp_handler = ftpserver.FTPHandler
    ftp_handler.banner = 'Rackspace Cloud Files %s using %s' % \
        (__version__, ftp_handler.banner)
    ftp_handler.authorizer = RackspaceCloudAuthorizer()
    ftp_handler.abstracted_fs = RackspaceCloudFilesFS

    ftpd = ftpserver.FTPServer(('', bind), ftp_handler)
    ftpd.serve_forever()
    

if __name__ == '__main__':
    main()
