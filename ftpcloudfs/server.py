#/usr/bin/env python
#
# Author: Chmouel Boudjnah <chmouel.boudjnah@rackspace.co.uk>
#
# Note: Not officially supported by Rackspace but I will be glad to
# help you if you send me an email or leave a comment (preferred
# method) on my blog: http://blog.chmouel.com
import os
import datetime
import time
import mimetypes

from pyftpdlib import ftpserver
import cloudfiles

from monkeypatching import ChunkObject


class CloudOperations(object):
    '''Storing connection object'''

    def __init__(self):
        self.connection = None
        self.username = None

    def authenticate(self, username, api_key, servicenet=False, authurl=None):
        self.username = username

        #Thanks the way get_connection works we don't have to check if
        #the python api version is accepting servicenet keyword
        kwargs = dict(servicenet=servicenet)
        #Only add authurl if the user asked for it, to maintain
        #compatibility with old python api versions
        if authurl:
            kwargs['authurl'] = authurl
        self.connection = cloudfiles.get_connection(username, api_key, **kwargs)

operations = CloudOperations()


class RackspaceCloudAuthorizer(ftpserver.DummyAuthorizer):
    '''FTP server authorizer. Logs the users into Rackspace Cloud
    Files and keeps track of them.
    '''
    users = {}
    servicenet = False
    authurl = None

    def validate_authentication(self, username, password):
        try:
            operations.authenticate(username, password,
                                    servicenet=self.servicenet,
                                    authurl=self.authurl)
            return True
        except(cloudfiles.errors.AuthenticationFailed):
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


class RackspaceCloudFilesFD(object):

    def __init__(self, username, container, obj, mode):
        self.username = username
        self.container = container
        self.name = obj
        self.mode = mode
        self.closed = False
        self.total_size = 0

        if not all([username, container, obj]):
            self.closed = True
            raise IOError(1, 'Operation not permitted')

        try:
            self.container = \
                operations.connection.get_container(self.container)
        except(cloudfiles.errors.NoSuchContainer):
            raise IOError(2, 'No such file or directory')

        if 'r' in self.mode:
            try:
                self.obj = self.container.get_object(self.name)
            except(cloudfiles.errors.NoSuchObject):
                raise IOError(2, 'No such file or directory')
        else: #write
            self.obj = ChunkObject(self.container, obj)
            self.obj.content_type = mimetypes.guess_type(obj)[0]
            self.obj.prepare_chunk()

    def write(self, data):
        if 'r' in self.mode:
            raise OSError(1, 'Operation not permitted')
        self.obj.send_chunk(data)

    def close(self):
        if 'r' in self.mode:
            return
        self.obj.finish_chunk()

    def read(self, size=65536):
        readsize = size
        if (self.total_size + size) > self.obj.size:
            readsize = self.obj.size - self.total_size
        if self.total_size >= self.obj.size:
            return
        else:
            offset = self.total_size
            self.total_size += size
            return self.obj.read(size=readsize, offset=offset)

    def seek(self, *kargs, **kwargs):
        #TODO: properly
        raise IOError(1, 'Operation not permitted')


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

    def open(self, filename, mode):
        #print '#### open', filename, mode
        username, container, obj = self.parse_fspath(filename)
        return RackspaceCloudFilesFD(username, container, obj, mode)

    def chdir(self, path):
        if path.startswith(self.root):
            _, container, obj = self.parse_fspath(path)

            if not container:
                self.cwd = self.fs2ftp(path)
                return

            if not obj:
                try:
                    operations.connection.get_container(container)
                    self.cwd = self.fs2ftp(path)
                    return
                except(cloudfiles.errors.NoSuchContainer,
                       cloudfiles.errors.InvalidContainerName):
                    raise OSError(2, 'No such file or directory')

        raise OSError(550, 'Failed to change directory.')

    def mkdir(self, path):
        try:
            _, container, obj = self.parse_fspath(path)
            if obj:
                raise OSError(1, 'Operation not permitted')
        except(ValueError):
            raise OSError(2, 'No such file or directory')

        operations.connection.create_container(container)

    def listdir(self, path):
        try:
            _, container, obj = self.parse_fspath(path)
        except(ValueError):
            raise OSError(2, 'No such file or directory')

        if not container and not obj:
            return operations.connection.list_containers()

        if container and not obj:
            try:
                cnt = operations.connection.get_container(container)
                return cnt.list_objects()
            except(cloudfiles.errors.NoSuchContainer):
                raise OSError(2, 'No such file or directory')

    def rmdir(self, path):
        _, container, name = self.parse_fspath(path)

        if name:
            raise OSError(13, 'Operation not permitted')

        try:
            container = operations.connection.get_container(container)
        except(cloudfiles.errors.NoSuchContainer):
            raise OSError(2, 'No such file or directory')

        try:
            operations.connection.delete_container(container)
        except(cloudfiles.errors.ContainerNotEmpty):
            raise OSError(39, "Directory not empty: '%s'" % container)

    def remove(self, path):
        _, container, name = self.parse_fspath(path)

        if not name:
            raise OSError(13, 'Operation not permitted')

        try:
            container = operations.connection.get_container(container)
            obj = container.get_object(name)
            container.delete_object(obj)
        except(cloudfiles.errors.NoSuchContainer,
               cloudfiles.errors.NoSuchObject):
            raise OSError(2, 'No such file or directory')
        return not name

    def rename(self, src, dst):
        raise OSError(1, 'Operation not permitted')

    def isfile(self, path):
        return not self.isdir(path)

    def islink(self, path):
        return False

    def isdir(self, path):
        _, _, name = self.parse_fspath(path)
        return not name

    def getsize(self, path):
        return self.stat(path).st_size

    def getmtime(self, path):
        return self.stat(path).st_mtime

    def realpath(self, path):
        return path

    def lexists(self, path):
        try:
            _, container, obj = self.parse_fspath(path)
        except(ValueError):
            raise OSError(2, 'No such file or directory')

        if not container and not obj:
            containers = operations.connection.list_containers()
            return container in containers

        if container and not obj:
            try:
                cnt = operations.connection.get_container(container)
                objects = cnt.list_objects()
            except(cloudfiles.errors.NoSuchContainer):
                raise OSError(2, 'No such file or directory')
            return obj in objects

    def stat(self, path):
        _, container, name = self.parse_fspath(path)
        if not name:
            raise OSError(40, 'unsupported')
        try:
            container = operations.connection.get_container(container)
            obj = container.get_object(name)
            size = obj.size
            #TODO: stats datetime
            return os.stat_result((666, 0L, 0L, 0, 0, 0, size, 0, 0, 0))
        except(cloudfiles.errors.NoSuchContainer,
               cloudfiles.errors.NoSuchObject):
            raise OSError(2, 'No such file or directory')

    exists = lexists
    lstat = stat

    def validpath(self, path):
        return True

    def get_list_dir(self, path):
        try:
            _, container, obj = self.parse_fspath(path)
        except(ValueError):
            raise OSError(2, 'No such file or directory')

        if not container and not obj:
            containers = operations.connection.list_containers_info()
            return self.format_list_containers(containers)

        if container and not obj:
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

    def get_stat_dir(self, *kargs, **kwargs):
        raise OSError(40, 'unsupported')

    def format_mlsx(self, *kargs, **kwargs):
        raise OSError(40, 'unsupported')
