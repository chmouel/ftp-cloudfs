"""
A filesystem like interface to cloudfiles

Author: Chmouel Boudjnah <chmouel.boudjnah@rackspace.co.uk>
Author: Nick Craig-Wood <nick@craig-wood.com>
"""

import os
import time
import mimetypes
import stat
import sys
import logging
from errno import EPERM, ENOENT, EACCES, ENOTEMPTY, ENOTDIR, EIO
import cloudfiles
from chunkobject import ChunkObject
from errors import IOSError
import posixpath

__all__ = ['CloudFilesFS']

def cfwrapper(fn, *args, **kwargs):
    '''Run fn(*args, **kwargs) catching and translating any cloudfiles errors into IOSErrors'''
    try:
        return fn(*args, **kwargs)
    except (cloudfiles.errors.NoSuchContainer,
            cloudfiles.errors.NoSuchObject), e:
        raise IOSError(ENOENT, 'Not found: %s' % e)
    except cloudfiles.errors.ContainerNotEmpty, e:
        raise IOSError(ENOTEMPTY, 'Directory not empty: %s')
    except cloudfiles.errors.ResponseError, e:
        logging.warning("Response error: %s" % e)
        # FIXME make some attempt to raise different errors on e.status
        raise IOSError(EPERM, 'Operation not permitted: %s' % e)
    except (cloudfiles.errors.AuthenticationError,
            cloudfiles.errors.AuthenticationFailed,
            cloudfiles.errors.ContainerNotPublic), e:
        logging.warning("Authentication error: %s: %s" % (e.__class__.__name__, e))
        raise IOSError(EPERM, 'Operation not permitted: %s' % e)
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

def parse_fspath(path):
    '''Returns a (container, path) tuple. For shorter paths
    replaces not provided values with empty strings.
    May raise IOSError for invalid paths
    '''
    if not path.startswith('/'):
        logging.warning('parse_fspath: You have to provide an absolute path: %r' % path)
        raise IOSError(ENOENT, 'Absolute path needed')
    parts = path.split('/', 2)[1:]
    while len(parts) < 2:
        parts.append('')
    return tuple(parts)

class CloudFilesFD(object):
    '''Acts like a file() object, but attached to a cloud files object'''

    def __init__(self, cffs, container, obj, mode):
        self.cffs = cffs
        self.container = container
        self.name = obj
        self.mode = mode
        self.closed = False
        self.total_size = 0

        if not all([container, obj]):
            self.closed = True
            raise IOSError(EPERM, 'Container and object requred')

        self.container = self.cffs._get_container(self.container)

        if 'r' in self.mode:
            self.obj = cfwrapper(self.container.get_object, self.name)
            logging.debug("read fd obj.name=%r obj.size=%r" % (self.obj.name, self.obj.size))
        else: #write
            self.obj = ChunkObject(self.container, obj)
            self.obj.content_type = mimetypes.guess_type(obj)[0]
            self.obj.prepare_chunk()

    def write(self, data):
        '''Write data to the object'''
        if 'r' in self.mode:
            raise IOSError(EPERM, "Can't write to stream opened for read")
        self.obj.send_chunk(data)

    def close(self):
        '''Close the object and finish the data transfer'''
        if 'r' in self.mode:
            return
        self.obj.finish_chunk()

    def read(self, size=65536):
        '''Read data from the object'''
        logging.debug("read size=%r, total_size=%r, obj.size=%r" % (size, self.total_size, self.obj.size))
        readsize = size
        if (self.total_size + size) > self.obj.size:
            readsize = self.obj.size - self.total_size
        if self.total_size >= self.obj.size:
            return ""
        else:
            offset = self.total_size
            self.total_size += size
            return self.obj.read(size=readsize, offset=offset)

    def seek(self, *kargs, **kwargs):
        '''Seek in the object: FIXME doesn't work and raises an error'''
        logging.debug("seek args=%s, kargs=%s" % (str(kargs), str(kwargs)))
        raise IOSError(EPERM, "Seek not implemented")

class ListDirCache(object):
    '''
    Cache for listdir.  This is to cache the very common case when we
    call listdir and then immediately call stat() on all the objects.
    In the OS this would be cached in the VFS but we have to make our
    own caching here to avoid the stat calls each making a connection.
    '''
    MAX_CACHE_TIME = 10         # seconds to cache the listdir for
    def __init__(self, cffs):
        self.cffs = cffs
        self.path = None
        self.cache = None
        self.when = time.time()

    def flush(self):
        '''Flush the listdir cache'''
        self.cache = None

    def _make_stat(self, last_modified=None, content_type="application/directory", count=1, bytes=0, **kwargs):
        '''Make a stat object from the parameters passed in from'''
        if last_modified:
            if "." in last_modified:
                last_modified, microseconds = last_modified.rsplit(".", 1)
                microseconds = float("0."+microseconds)
            else:
                microseconds = 0.0
            mtime_tuple = list(time.strptime(last_modified, "%Y-%m-%dT%H:%M:%S"))
            mtime_tuple[8] = 0  # Use GMT
            mtime = time.mktime(mtime_tuple) + microseconds
        else:
            mtime = time.time()
        if content_type == "application/directory":
            mode = 0755|stat.S_IFDIR
        else:
            mode = 0644|stat.S_IFREG
        #(mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime)
        return os.stat_result((mode, 0L, 0L, count, 0, 0, bytes, mtime, mtime, mtime))

    def listdir_container(self, cache, container, path=""):
        '''Fills cache with the list dir of the container'''
        logging.debug("listdir container %r path %r" % (container, path))
        cnt = self.cffs._get_container(container)
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
            obj['count'] = 1
            # Keep all names in utf-8, just like the filesystem
            name = posixpath.basename(obj['name']).encode("utf-8")
            cache[name] = self._make_stat(**obj)

    def listdir_root(self, cache):
        '''Fills cache with the list of containers'''
        logging.debug("listdir root")
        objects = cfwrapper(self.cffs.connection.list_containers_info)
        for obj in objects:
            # {u'count': 0, u'bytes': 0, u'name': u'container1'},
            # Keep all names in utf-8, just like the filesystem
            name = obj['name'].encode("utf-8")
            cache[name] = self._make_stat(**obj)

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

    def listdir_with_stat(self, path):
        '''Return the directory list of the path with stat objects for
        each, filling the cache in the process, as a list of tuples
        (leafname, stat_result)'''
        self.listdir(path)
        return sorted(self.cache.iteritems())

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
        directory, leaf = posixpath.split(path)
        # Refresh the cache it if is old, or wrong
        if not self.valid(directory):
            self.listdir(directory)
        if path != "":
            try:
                stat_info = self.cache[leaf]
            except KeyError:
                logging.debug("Didn't find %r in directory listing" % leaf)
                raise IOSError(ENOENT, 'No such file or directory %s' % leaf)
        else:
            # Root directory size is sum of containers, count is containers
            bytes = sum(stat_info.st_size for stat_info in self.cache.values())
            count = len(self.cache)
            stat_info = self._make_stat(count=count, bytes=bytes)
        return stat_info
    
class CloudFilesFS(object):
    '''Cloud Files File system emulation

    All the methods on this class emulate os.* or os.path.* functions
    of the same name.
    '''

    def __init__(self, username, api_key, servicenet=False, authurl=None):
        '''
        Open the Cloudfiles connection

        username - if None then don't make the connection
        api_key
        servicenet - use the Rackspace internal network
        authurl - for use with OpenStack
        '''
        self.username = username
        self.connection = None
        self.servicenet = servicenet
        self.authurl = authurl
        if username is not None:
            self.authenticate(username, api_key)
        # A cache to hold the information from the last listdir
        self._listdir_cache = ListDirCache(self)
        self._cwd = '/'

    def authenticate(self, username, api_key):
        '''
        Authenticates and opens the connection
        '''
        # Thanks the way get_connection works we don't have to check if
        # the python api version is accepting servicenet keyword
        kwargs = dict(servicenet=self.servicenet)
        # Only add authurl if the user asked for it, to maintain
        # compatibility with old python api versions
        if self.authurl:
            kwargs['authurl'] = self.authurl
        self.connection = cfwrapper(cloudfiles.get_connection, username, api_key, **kwargs)

    def close(self):
        '''Dummy function which does nothing - no need to close'''
        pass

    def _get_container(self, container):
        '''Gets the named container returning a container object, raising
        IOSError if not found'''
        return cfwrapper(self.connection.get_container, container)

    def isabs(self, path):
        """Test whether a path is absolute"""
        return posixpath.isabs(path)

    def normpath(self, path):
        """Normalize path, eliminating double slashes, etc."""
        return posixpath.normpath(path)

    def abspath(self, path):
        """Return an absolute path."""
        if not self.isabs(path):
            path = posixpath.join(self.getcwd(), path)
        return self.normpath(path)

    def open(self, path, mode):
        '''Open path with mode, raise IOError on error'''
        path = self.abspath(path)
        logging.debug("open %r mode %r" % (path, mode))
        self._listdir_cache.flush()
        container, obj = parse_fspath(path)
        return CloudFilesFD(self, container, obj, mode)

    def chdir(self, path):
        '''Change current directory, raise OSError on error'''
        path = self.abspath(path)
        logging.debug("chdir %r" % path)
        if not path.startswith("/"):
            raise IOSError(ENOENT, 'Failed to change directory.')
        container, obj = parse_fspath(path)
        if not container:
            logging.debug("cd to /")
        else:
            logging.debug("cd to container %r directory %r" % (container, obj))
            if not self.isdir(path):
                raise IOSError(ENOTDIR, "Can't cd to a file")
        self._cwd = path

    def getcwd(self):
        '''Returns the current working directory'''
        return self._cwd

    def mkdir(self, path):
        '''Make a directory, raise OSError on error'''
        path = self.abspath(path)
        logging.debug("mkdir %r" % path)
        self._listdir_cache.flush()
        container, obj = parse_fspath(path)
        if obj:
            logging.debug("Making directory %r in %r" % (obj, container))
            cnt = self._get_container(container)
            directory_obj = cnt.create_object(obj)
            directory_obj.content_type = "application/directory"
            directory_obj.write("")
        else:
            logging.debug("Making container %r" % (container,))
            self.connection.create_container(container)

    def listdir(self, path):
        '''List a directory, raise OSError on error'''
        path = self.abspath(path)
        logging.debug("listdir %r" % path)
        return self._listdir_cache.listdir(path)

    def listdir_with_stat(self, path):
        '''Return the directory list of the path with stat objects for
        each, filling the cache in the process, as a list of tuples
        (leafname, stat_result)'''
        path = self.abspath(path)
        logging.debug("listdir_with_stat %r" % path)
        return self._listdir_cache.listdir_with_stat(path)

    def rmdir(self, path):
        '''Remove a directory, raise OSError on error'''
        path = self.abspath(path)
        logging.debug("rmdir %r" % path)
        self._listdir_cache.flush()
        container, obj = parse_fspath(path)

        if not self.isdir(path):
            if self.isfile(path):
                raise IOSError(ENOTDIR, "Not a directory")
            raise IOSError(ENOENT, 'No such file or directory')
            
        if self.listdir(path):
            raise IOSError(ENOTEMPTY, "Directory not empty: '%s'" % path)

        cnt = self._get_container(container)

        if obj:
            logging.debug("Removing directory %r in %r" % (obj, container))
            cfwrapper(cnt.delete_object, obj)
        else:
            logging.debug("Removing container %r" % (container,))
            cfwrapper(self.connection.delete_container, container)

    def remove(self, path):
        '''Remove a file, raise OSError on error'''
        path = self.abspath(path)
        logging.debug("remove %r" % path)
        self._listdir_cache.flush()
        container, name = parse_fspath(path)

        if not name:
            raise IOSError(EACCES, "Can't remove a container")

        container = self._get_container(container)
        obj = cfwrapper(container.get_object, name)
        cfwrapper(container.delete_object, obj)
        return not name

    def _rename_container(self, src_container_name, dst_container_name):
        '''Rename src_container_name into dst_container_name'''
        logging.debug("rename container %r -> %r" % (src_container_name, dst_container_name))
        # Delete the old container first, raising error if not empty
        cfwrapper(self.connection.delete_container, src_container_name)
        cfwrapper(self.connection.create_container, dst_container_name)

    def rename(self, src, dst):
        '''Rename a file/directory from src to dst, raise OSError on error'''
        src = self.abspath(src)
        dst = self.abspath(dst)
        logging.debug("rename %r -> %r" % (src, dst))
        self._listdir_cache.flush()
        # Check not renaming to itself
        if src == dst:
            logging.debug("Renaming %r to itself - doing nothing" % src)
            return
        # If dst is an existing directory, copy src inside it
        if self.isdir(dst):
            if dst:
                dst += "/"
            dst += posixpath.basename(src)
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
            return self._rename_container(src_container_name, dst_container_name)
        # ...otherwise can't deal with root stuff
        if not src_container_name or not src_path or not dst_container_name or not dst_path:
            logging.info("Can't rename %r -> %r" % (src, dst))
            raise IOSError(EACCES, "Can't rename to / from root")
        # Check destination directory exists
        if not self.isdir(posixpath.split(dst)[0]):
            logging.info("Can't copy %r -> %r dst directory doesn't exist" % (src, dst))
            raise IOSError(ENOENT, "Can't copy %r -> %r dst directory doesn't exist" % (src, dst))
        # Do the rename of the file/dir
        src_container = self._get_container(src_container_name)
        dst_container = self._get_container(dst_container_name)
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
        return self.abspath(path)

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
        path = self.abspath(path)
        logging.debug("stat %r" % path)
        return self._listdir_cache.stat(path)

    exists = lexists
    lstat = stat

    def validpath(self, path):
        '''Check whether the path belongs to user's home directory'''
        return True
