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
import stat
import sys
import logging
from errno import EPERM, ENOENT, EACCES, ENOTEMPTY, ENOTDIR, EIO

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

def cfwrapper(fn, *args, **kwargs):
    '''Run fn(*args, **kwargs) catching and translating any cloudfiles errors into IOSErrors'''
    try:
        return fn(*args, **kwargs)
    except (cloudfiles.errors.NoSuchContainer,
            cloudfiles.errors.NoSuchObject):
        raise IOSError(ENOENT, 'No such file or directory')
    except cloudfiles.errors.ContainerNotEmpty, e:
        raise IOSError(ENOTEMPTY, 'Directory not empty: %s' % e)
    except cloudfiles.errors.ResponseError, e:
        logging.warning("Response error: %s" % e)
        # FIXME make some attempt to raise different errors on e.status
        raise IOSError(EPERM, 'Operation not permitted: %s' % e)
    except (cloudfiles.errors.AuthenticationError,
            cloudfiles.errors.AuthenticationFailed,
            cloudfiles.errors.ContainerNotPublic):
        raise IOSError(EPERM, 'Operation not permitted')
    # All the remaining cloudfiles errors.  There is no superclass
    # otherwise we could have caught that!
    except (cloudfiles.errors.CDNNotEnabled,
            cloudfiles.errors.IncompleteSend,
            cloudfiles.errors.InvalidContainerName,
            cloudfiles.errors.InvalidMetaName,
            cloudfiles.errors.InvalidMetaValue,
            cloudfiles.errors.InvalidObjectName,
            cloudfiles.errors.InvalidObjectSize,
            cloudfiles.errors.InvalidUrl), e:
        logging.warning("Unexpected cloudfiles error: %s" % e)
        raise IOSError(EIO, 'Unexpected cloudfiles error')

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
        return cfwrapper(self.connection.get_container, container)

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
            cfwrapper(operations.authenticate,
                      username, password,
                      servicenet=self.servicenet, authurl=self.authurl)
            return True
        except EnvironmentError:
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
            self.obj = cfwrapper(self.container.get_object, self.name)
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

def parse_fspath(path):
    '''Returns a (container, path) tuple. For shorter paths
    replaces not provided values with empty strings.
    May raise IOSError for invalid paths
    '''
    if not path.startswith(os.sep):
        logging.warning('parse_fspath: You have to provide an absolute path: %r' % path)
        raise IOSError(ENOENT, 'No such file or directory')
    parts = path.split(os.sep, 2)[1:]
    while len(parts) < 2:
        parts.append('')
    return tuple(parts)

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
        self.path = None
        self.cache = None
        self.when = time.time()

    def flush(self):
        '''Flush the listdir cache'''
        self.cache = None

    def listdir_container(self, cache, container, path=""):
        '''Fills cache with the list dir of the container'''
        logging.debug("listdir container %r path %r" % (container, path))
        cnt = operations.get_container(container)
        if path:
            prefix = path.rstrip("/")+"/"
        else:
            prefix = None
        objects = cnt.list_objects_info(prefix=prefix, delimiter="/")
        for obj in objects:
            # {u'bytes': 4820,  u'content_type': '...',  u'hash': u'...',  u'last_modified': u'2008-11-05T00:56:00.406565',  u'name': u'new_object'},
            if 'subdir' in obj:
                # {u'subdir': 'dirname'}
                obj['name'] = obj['subdir'].rstrip("/")
                obj['bytes'] = 0
            obj['count'] = 1
            # Keep all names in utf-8, just like the filesystem
            name = basename(obj['name']).encode("utf-8")
            cache[name] = obj

    def listdir_root(self, cache):
        '''Fills cache with the list of containers'''
        logging.debug("listdir root")
        objects = cfwrapper(operations.connection.list_containers_info)
        for obj in objects:
            # {u'count': 0, u'bytes': 0, u'name': u'container1'},
            # Keep all names in utf-8, just like the filesystem
            name = obj['name'].encode("utf-8")
            cache[name] = obj

    def listdir(self, path):
        '''Return the directory list of the path, filling the cache in the process'''
        path = path.rstrip("/")
        logging.debug("listdir %r" % path)
        self.flush()
        cache = {}
        if path == "":
            self.cache = self.listdir_root(cache)
        else:
            container, obj = parse_fspath(path)
            self.cache = self.listdir_container(cache, container, obj)
        self.cache = cache
        self.path = path
        self.when = time.time()
        leaves = sorted(self.cache.keys())
        logging.debug(".. %r" % leaves)
        return leaves

    def valid(self, path):
        '''Check the cache is valid for the container and directory path'''
        if not self.cache:
            return False
        if self.path != path:
            return False
        age = time.time() - self.when
        return age < self.MAX_CACHE_TIME

    def stat(self, path):
        '''Returns an os.stat_result for path or raises IOSError

        Returns the information from the cache if possible
        '''
        path = path.rstrip("/")
        logging.debug("stat path %r" % (path))
        directory, leaf = path_split(path)
        # Refresh the cache it if is old, or wrong
        if not self.valid(directory):
            self.listdir(directory)
        if path != "":
            try:
                obj = self.cache[leaf]
            except KeyError:
                logging.warning("Should have found %r in directory listing" % leaf)
                raise IOSError(ENOENT, 'No such file or directory')
        else:
            # Root directory size is sum of containers, count is containers
            bytes = sum(obj['bytes'] for obj in self.cache.values())
            count = len(self.cache)
            obj = dict(count=count, bytes=bytes)
        if 'last_modified' in obj:
            mtime_tuple = time.strptime(obj['last_modified'], LAST_MODIFIED_FORMAT)
            mtime = time.mktime(mtime_tuple)
        else:
            mtime = time.time()
        if obj.get('content_type', "application/directory") == "application/directory":
            mode = 0755|stat.S_IFDIR
        else:
            mode = 0644|stat.S_IFREG
        #(mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime)
        return os.stat_result((mode, 0L, 0L, obj['count'], 0, 0, obj['bytes'], mtime, mtime, mtime))
    
class RackspaceCloudFilesFS(ftpserver.AbstractedFS):
    '''Rackspace Cloud Files File system emulation for FTP server.
    '''

    def __init__(self, *args, **kwargs):
        super(RackspaceCloudFilesFS, self).__init__(*args, **kwargs)
        # A cache to hold the information from the last listdir
        self.listdir_cache = ListDirCache()

    def open(self, path, mode):
        '''Open path with mode, raise IOError on error'''
        logging.debug("open %r mode %r" % (path, mode))
        self.listdir_cache.flush()
        container, obj = parse_fspath(path)
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
        container, obj = parse_fspath(path)
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
        self.listdir_cache.flush()
        container, obj = parse_fspath(path)
        if obj:
            logging.debug("Making directory %r in %r" % (obj, container))
            cnt = operations.get_container(container)
            directory_obj = cnt.create_object(obj)
            directory_obj.content_type = "application/directory"
            directory_obj.write("")
        else:
            logging.debug("Making container %r" % (container,))
            operations.connection.create_container(container)

    def listdir(self, path):
        '''List a directory, raise OSError on error'''
        logging.debug("listdir %r" % path)
        return self.listdir_cache.listdir(path)

    def rmdir(self, path):
        '''Remove a directory, raise OSError on error'''
        logging.debug("rmdir %r" % path)
        self.listdir_cache.flush()
        container, obj = parse_fspath(path)

        if not self.isdir(path):
            if self.isfile(path):
                raise IOSError(ENOTDIR, "Not a directory")
            raise IOSError(ENOENT, 'No such file or directory')
            
        if self.listdir(path):
            raise IOSError(ENOTEMPTY, "Directory not empty: '%s'" % path)

        cnt = operations.get_container(container)

        if obj:
            logging.debug("Removing directory %r in %r" % (obj, container))
            cfwrapper(cnt.delete_object, obj)
        else:
            logging.debug("Removing container %r" % (container,))
            cfwrapper(operations.connection.delete_container, container)

    def remove(self, path):
        '''Remove a file, raise OSError on error'''
        logging.debug("remove %r" % path)
        self.listdir_cache.flush()
        container, name = parse_fspath(path)

        if not name:
            raise IOSError(EACCES, 'Operation not permitted')

        container = operations.get_container(container)
        obj = cfwrapper(container.get_object, name)
        cfwrapper(container.delete_object, obj)
        return not name

    def rename_container(self, src_container_name, dst_container_name):
        '''Rename src_container_name into dst_container_name'''
        logging.debug("rename container %r -> %r" % (src_container_name, dst_container_name))
        # Delete the old container first, raising error if not empty
        cfwrapper(operations.connection.delete_container, src_container_name)
        cfwrapper(operations.connection.create_container, dst_container_name)

    def rename(self, src, dst):
        '''Rename a file/directory from src to dst, raise OSError on error'''
        logging.debug("rename %r -> %r" % (src, dst))
        self.listdir_cache.flush()
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
        src_container_name, src_path = parse_fspath(src)
        dst_container_name, dst_path = parse_fspath(dst)
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
        src_obj = cfwrapper(src_container.get_object, src_path)
        # Copy src -> dst
        cfwrapper(src_obj.copy_to, dst_container_name, dst_path)
        # Delete dst
        src_container.delete_object(src_path)

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
        return self.listdir_cache.stat(path)

    exists = lexists
    lstat = stat

    def validpath(self, path):
        '''Check whether the path belongs to user's home directory'''
        return True
