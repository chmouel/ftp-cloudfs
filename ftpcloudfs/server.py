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
import rfc822
import stat

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

class ListDirCache(object):
    '''Cache for listdir'''
    MAX_CACHE_TIME = 10         # seconds to cache the listdir for
    def __init__(self):
        self.container = None
        self.cache = None
        self.when = time.time()
    def listdir(self, container):
        '''Returns the list dir of the container and fills the cache'''
        cnt = operations.connection.get_container(container)
        objects = cnt.list_objects_info()
        self.container = container
        self.when = time.time()
        # FIXME the encode("utf-8") is a bodge for a python-cloudfiles
        # Which returns unicode strings in list_objects_info, but
        # utf-8 is needed in get_container
        self.cache = dict((o['name'].encode("utf-8"), o) for o in objects)
        return sorted(self.cache.keys())
    def stat(self, container, name):
        '''Returns (size, mtime) for name in container or raises
        cloudfiles.errors.NoSuchObject
        Returns the information from the cache if possible
        '''
        age = time.time() - self.when
        if self.container == container and age < self.MAX_CACHE_TIME:
            # Read info from listdir cache
            try:
                obj = self.cache[name]
            except KeyError:
                raise cloudfiles.errors.NoSuchObject()
            size = obj['bytes']
            mtime_tuple = time.strptime(obj['last_modified'], '%Y-%m-%dT%H:%M:%S.%f')
        else:
            # Read info direct from container
            container = operations.connection.get_container(container)
            obj = container.get_object(name)
            size = obj.size
            mtime_tuple = rfc822.parsedate(obj.last_modified)
        if mtime_tuple:
            mtime = time.mktime(mtime_tuple)
        else:
            mtime = 0
        return (size, mtime)
    
class RackspaceCloudFilesFS(ftpserver.AbstractedFS):
    '''Rackspace Cloud Files File system emulation for FTP server.
    '''

    def __init__(self, *args, **kwargs):
        super(RackspaceCloudFilesFS, self).__init__(*args, **kwargs)
        # A cache to hold the information from the last listdir
        self.listdir_cache = ListDirCache()

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

    def _set_cwd(self, new_cwd):
        '''Set the self.cwd

        In pyftpd >= 0.6 it is a property backed by self._cwd, in <
        0.6 it is just an attribute self.cwd
        '''
        try:
            self.cwd = new_cwd
        except AttributeError:
            self._cwd = new_cwd

    def chdir(self, path):
        if path.startswith(self.root):
            _, container, obj = self.parse_fspath(path)

            if not container:
                self._set_cwd(self.fs2ftp(path))
                return

            if not obj:
                try:
                    operations.connection.get_container(container)
                    self._set_cwd(self.fs2ftp(path))
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

        if not container:
            try:
                return operations.connection.list_containers()
            except(cloudfiles.errors.ResponseError):
                raise OSError(1, 'Operation not permitted')
        else:
            try:
                return self.listdir_cache.listdir(container)
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
            mtime = time.time()
            return os.stat_result((0755|stat.S_IFDIR, 0L, 0L, 1, 0, 0, 4096, mtime, mtime, mtime))
        try:
            size, mtime = self.listdir_cache.stat(container, name)
            #(mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime)
            return os.stat_result((0666, 0L, 0L, 1, 0, 0, size, mtime, mtime, mtime))
        except(cloudfiles.errors.NoSuchContainer,
               cloudfiles.errors.NoSuchObject):
            raise OSError(2, 'No such file or directory')

    exists = lexists
    lstat = stat

    def validpath(self, path):
        return True
