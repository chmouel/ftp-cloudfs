#/usr/bin/env python
#
# Author: Chmouel Boudjnah <chmouel.boudjnah@rackspace.co.uk>
#
# Note: Not officially supported by Rackspace but I will be glad to
# help you if you send me an email or leave a comment (preferred
# method) on my blog: http://blog.chmouel.com
import os
import time
import mimetypes
import rfc822
import stat
import sys
import logging
from errno import EPERM, ENOENT, EACCES, ENOTEMPTY, ENOTDIR

from pyftpdlib import ftpserver
import cloudfiles

from monkeypatching import ChunkObject

sysinfo = sys.version_info
LAST_MODIFIED_FORMAT="%Y-%m-%dT%H:%M:%S.%f"
if sysinfo[0] <= 2 and sysinfo[1] <= 5:
    LAST_MODIFIED_FORMAT="%Y-%m-%dT%H:%M:%S"

class IOSError(OSError, IOError):
    '''Subclass of OSError and IOError

    This is needed because pyftpdlib catches either OSError, or
    IOError depending on which operation it is performing, which is
    perfectly correct, but makes our life more difficult.

    However our operations don't map to simple functions, and have
    common infrastructure.  These common infrastructure functions can
    be called from either context and so don't know which error to
    raise.

    Using this combined type everywhere fixes the problem at very
    small cost (multiple inheritance!)'''

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

    def get_container(self, container):
        '''Gets the named container returning a container object, raising
        IOSError if not found'''
        try:
            return self.connection.get_container(container)
        except cloudfiles.errors.NoSuchContainer:
            raise IOSError(ENOENT, 'No such file or directory')

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
        return os.sep

    def get_msg_login(self, username):
        return 'Welcome %s' % username

    def get_msg_quit(self, username):
        return 'Goodbye %s' % username


class RackspaceCloudFilesFD(object):
    '''Acts like a file() object, but attached to a cloud files object'''

    def __init__(self, container, obj, mode):
        self.container = container
        self.name = obj
        self.mode = mode
        self.closed = False
        self.total_size = 0

        if not all([container, obj]):
            self.closed = True
            raise IOSError(EPERM, 'Operation not permitted')

        self.container = operations.get_container(self.container)

        if 'r' in self.mode:
            try:
                self.obj = self.container.get_object(self.name)
            except(cloudfiles.errors.NoSuchObject):
                raise IOSError(ENOENT, 'No such file or directory')
        else: #write
            self.obj = ChunkObject(self.container, obj)
            self.obj.content_type = mimetypes.guess_type(obj)[0]
            self.obj.prepare_chunk()

    def write(self, data):
        if 'r' in self.mode:
            raise IOSError(EPERM, 'Operation not permitted')
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
        raise IOSError(EPERM, 'Operation not permitted')

def path_split(path):
    '''
    Split a pathname.  Returns tuple "(head, tail)" where "tail" is
    everything after the final slash.  If there is no slash, then it
    returns ("", path)
    '''
    if "/" in path:
        return path.rsplit("/", 1)
    return ("", path)

def basename(path):
    '''Returns the final component of a pathname'''
    return path_split(path)[1]

class ListDirCache(object):
    '''
    Cache for listdir.  This is to cache the very common case when we
    call listdir and then immediately call stat() on all the objects.
    In the OS this would be cached in the VFS but we have to make our
    own caching here to avoid the stat calls each making a connection.
    '''
    MAX_CACHE_TIME = 10         # seconds to cache the listdir for
    def __init__(self):
        self.container = None
        self.path = None
        self.cache = None
        self.when = time.time()
    def flush(self):
        '''Flush the listdir cache'''
        self.cache = None
    def listdir(self, container, path=""):
        '''Returns the list dir of the container and fills the cache'''
        logging.debug("listdir container %r path %r" % (container, path))
        cnt = operations.get_container(container)
        objects = cnt.list_objects_info(path=path, delimiter="/")
        self.container = container
        self.path = path
        self.when = time.time()
        self.cache = dict((basename(o['name']), o) for o in objects)
        leaves = sorted(self.cache.keys())
        logging.debug(".. %r" % leaves)
        # FIXME the encode("utf-8") is a bodge for a python-cloudfiles
        # Which returns unicode strings in list_objects_info, but
        # utf-8 is needed in get_container
        return [path.encode("utf-8") for path in leaves]
    def valid(self, container, path):
        '''Check the cache is valid for the container and directory path'''
        if not self.cache:
            return False
        if self.container != container or self.path != path:
            return False
        age = time.time() - self.when
        return age < self.MAX_CACHE_TIME
    def stat(self, container, path):
        '''Returns (size, mtime, is_directory) for path in container or raises
        OSError

        Returns the information from the cache if possible
        '''
        directory, leaf = path_split(path)
        logging.debug("stat container %r, path %r, directory %r" % (container, path, directory))
        if self.valid(container, directory):
            # Read info from listdir cache
            try:
                obj = self.cache[leaf]
            except KeyError:
                raise IOSError(ENOENT, 'No such file or directory')
            size = obj['bytes']
            mtime_tuple = time.strptime(obj['last_modified'], LAST_MODIFIED_FORMAT)
            content_type = obj['content_type']
        else:
            # Read info direct from container
            cnt = operations.get_container(container)
            try:
                obj = cnt.get_object(path)
            except cloudfiles.errors.NoSuchObject:
                raise IOSError(ENOENT, 'No such file or directory')
            size = obj.size
            mtime_tuple = rfc822.parsedate(obj.last_modified)
            content_type = obj.content_type
        if mtime_tuple:
            mtime = time.mktime(mtime_tuple)
        else:
            mtime = 0
        return (size, mtime, content_type == "application/directory")
    
class RackspaceCloudFilesFS(ftpserver.AbstractedFS):
    '''Rackspace Cloud Files File system emulation for FTP server.
    '''

    def __init__(self, *args, **kwargs):
        super(RackspaceCloudFilesFS, self).__init__(*args, **kwargs)
        # A cache to hold the information from the last listdir
        self.listdir_cache = ListDirCache()

    def parse_fspath(self, path):
        '''Returns a (container, path) tuple. For shorter paths
        replaces not provided values with empty strings.
        May raise IOSError for invalid paths
        '''
        if not path.startswith(os.sep):
            logging.warning('parse_fspath: You have to provide a full path: %r' % path)
            raise IOSError(ENOENT, 'No such file or directory')
        parts = path.split(os.sep, 2)[1:]
        while len(parts) < 2:
            parts.append('')
        return tuple(parts)

    def open(self, path, mode):
        '''Open path with mode, raise IOError on error'''
        logging.debug("open %r mode %r" % (path, mode))
        container, obj = self.parse_fspath(path)
        self.listdir_cache.flush()
        return RackspaceCloudFilesFD(container, obj, mode)

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
        '''Change current directory, raise OSError on error'''
        logging.debug("chdir %r" % path)
        if not path.startswith(self.root):
            raise IOSError(ENOENT, 'Failed to change directory.')
        container, obj = self.parse_fspath(path)
        if not container:
            logging.debug("cd to /")
        else:
            logging.debug("cd to container %r directory %r" % (container, obj))
            if not self.isdir(path):
                raise IOSError(ENOTDIR, "Can't cd to a directory")
        self._set_cwd(self.fs2ftp(path))

    def mkdir(self, path):
        '''Make a directory, raise OSError on error'''
        logging.debug("mkdir %r" % path)
        container, obj = self.parse_fspath(path)
        if obj:
            logging.debug("Making directory %r in %r" % (obj, container))
            cnt = operations.get_container(container)
            directory_obj = cnt.create_object(obj)
            directory_obj.content_type = "application/directory"
            directory_obj.write("")
            self.listdir_cache.flush()
        else:
            logging.debug("Making container %r" % (container,))
            operations.connection.create_container(container)

    def listdir(self, path):
        '''List a directory, raise OSError on error'''
        logging.debug("listdir %r" % path)
        container, obj = self.parse_fspath(path)
        if not container:
            try:
                return operations.connection.list_containers()
            except(cloudfiles.errors.ResponseError):
                raise IOSError(EPERM, 'Operation not permitted')
        else:
            try:
                return self.listdir_cache.listdir(container, obj)
            except(cloudfiles.errors.NoSuchContainer):
                raise IOSError(ENOENT, 'No such file or directory')

    def rmdir(self, path):
        '''Remove a directory, raise OSError on error'''
        logging.debug("rmdir %r" % path)
        container, obj = self.parse_fspath(path)

        if not self.isdir(path):
            if self.isfile(path):
                raise IOSError(ENOTDIR, "Not a directory")
            raise IOSError(ENOENT, 'No such file or directory')
            
        if self.listdir(path):
            raise IOSError(ENOTEMPTY, "Directory not empty: '%s'" % path)

        cnt = operations.get_container(container)

        if obj:
            logging.debug("Removing directory %r in %r" % (obj, container))
            try:
                cnt.delete_object(obj)
            except(cloudfiles.errors.ResponseError):
                raise IOSError(ENOTEMPTY, "Directory not empty: '%s'" % container)
            self.listdir_cache.flush()
        else:
            logging.debug("Removing container %r" % (container,))
            try:
                operations.connection.delete_container(container)
            except(cloudfiles.errors.ContainerNotEmpty):
                raise IOSError(ENOTEMPTY, "Directory not empty: '%s'" % container)

    def remove(self, path):
        '''Remove a file, raise OSError on error'''
        logging.debug("remove %r" % path)
        container, name = self.parse_fspath(path)

        if not name:
            raise IOSError(EACCES, 'Operation not permitted')

        container = operations.get_container(container)
        try:
            obj = container.get_object(name)
            container.delete_object(obj)
        except(cloudfiles.errors.NoSuchContainer,
               cloudfiles.errors.NoSuchObject):
            raise IOSError(ENOENT, 'No such file or directory')
        self.listdir_cache.flush()
        return not name

    def rename_container(self, src_container_name, dst_container_name):
        '''Rename src_container_name into dst_container_name'''
        logging.debug("rename container %r -> %r" % (src_container_name, dst_container_name))
        # Delete the old container first, raising error if not empty
        try:
            operations.connection.delete_container(src_container_name)
        except(cloudfiles.errors.ContainerNotEmpty):
            raise IOSError(ENOTEMPTY, "Directory not empty: '%s'" % src_container_name)
        operations.connection.create_container(dst_container_name)

    def rename(self, src, dst):
        '''Rename a file/directory from src to dst, raise OSError on error'''
        logging.debug("rename %r -> %r" % (src, dst))
        # Check not renaming to itself
        if src == dst:
            logging.debug("Renaming %r to itself - doing nothing" % src)
            return
        # If dst is an existing directory, copy src inside it
        if self.isdir(dst):
            if dst:
                dst += "/"
            dst += basename(src)
        # Check constraints for renaming a directory
        if self.isdir(src):
            if self.listdir(src):
                raise IOSError(ENOTEMPTY, "Can't rename non-empty directory: '%s'" % src)
            if self.isfile(dst):
                raise IOSError(ENOTDIR, "Can't rename directory to file")
        # Check not renaming to itself
        if src == dst:
            logging.debug("Renaming %r to itself - doing nothing" % src)
            return
        # Parse the paths now
        src_container_name, src_path = self.parse_fspath(src)
        dst_container_name, dst_path = self.parse_fspath(dst)
        logging.debug("`.. %r/%r -> %r/%r" % (src_container_name, src_path, dst_container_name, dst_path))
        # Check if we are renaming containers
        if not src_path and not dst_path and src_container_name and dst_container_name:
            return self.rename_container(src_container_name, dst_container_name)
        # ...otherwise can't deal with root stuff
        if not src_container_name or not src_path or not dst_container_name or not dst_path:
            logging.info("Can't rename %r -> %r" % (src, dst))
            raise IOSError(EACCES, "Can't rename to / from root")
        # Check destination directory exists
        if not self.isdir(path_split(dst)[0]):
            logging.info("Can't copy %r -> %r dst directory doesn't exist" % (src, dst))
            raise IOSError(ENOENT, 'No such file or directory')
        # Do the rename of the file/dir
        src_container = operations.get_container(src_container_name)
        dst_container = operations.get_container(dst_container_name)
        try:
            src_obj = src_container.get_object(src_path)
        except(cloudfiles.errors.NoSuchObject):
            raise IOSError(ENOENT, 'No such file or directory')
        # Copy src -> dst
        try:
            src_obj.copy_to(dst_container_name, dst_path)
        except(cloudfiles.errors.ResponseError), e:
            logging.debug("Copy failed %r" % e)
            raise IOSError(ENOENT, 'No such file or directory')
        # Delete dst
        src_container.delete_object(src_path)
        self.listdir_cache.flush()

    def isfile(self, path):
        '''Is this path a file.  Shouldn't raise an error if not found like os.path.isfile'''
        logging.debug("isfile %r" % path)
        try:
            return stat.S_ISREG(self.stat(path).st_mode)
        except EnvironmentError:
            return False

    def islink(self, path):
        '''Is this path a link.  Shouldn't raise an error if not found like os.path.islink'''
        logging.debug("islink %r" % path)
        return False

    def isdir(self, path):
        '''Is this path a directory.  Shouldn't raise an error if not found like os.path.isdir'''
        logging.debug("isdir %r" % path)
        try:
            return stat.S_ISDIR(self.stat(path).st_mode)
        except EnvironmentError:
            return False

    def getsize(self, path):
        '''Return the size of path, raise OSError on error'''
        logging.debug("getsize %r" % path)
        return self.stat(path).st_size

    def getmtime(self, path):
        '''Return the modification time of path, raise OSError on error'''
        logging.debug("getmtime %r" % path)
        return self.stat(path).st_mtime

    def realpath(self, path):
        '''Return the canonical path of the specified path'''
        return path

    def lexists(self, path):
        '''Test whether a path exists.  Returns True for broken symbolic links'''
        logging.debug("lexists %r" % path)
        try:
            self.stat(path)
            return True
        except EnvironmentError:
            return False

    def stat(self, path):
        '''Return os.stat_result object for path, raise OSError on error'''
        logging.debug("stat %r" % path)
        container, name = self.parse_fspath(path)
        logging.debug("`..container %r path %r" % (container, name))
        mode = 0755|stat.S_IFDIR
        if not name:
            if container:
                # If not root check container exists or not
                operations.get_container(container)
            mtime = time.time()
            return os.stat_result((mode, 0L, 0L, 1, 0, 0, 4096, mtime, mtime, mtime))
        size, mtime, is_dir = self.listdir_cache.stat(container, name)
        logging.debug("`...size = %r, mtime = %r, is_dir = %r" % (size, mtime, is_dir))
        #(mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime)
        if not is_dir:
            mode = 0644|stat.S_IFREG
        return os.stat_result((mode, 0L, 0L, 1, 0, 0, size, mtime, mtime, mtime))

    exists = lexists
    lstat = stat

    def validpath(self, path):
        '''Check whether the path belongs to user's home directory'''
        return True
