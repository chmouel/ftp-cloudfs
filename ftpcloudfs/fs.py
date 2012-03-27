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
from cloudfiles import Connection
from chunkobject import ChunkObject
from errors import IOSError
import posixpath
from constants import cloudfiles_api_timeout
from functools import wraps
import memcache
try:
    from hashlib import md5
except:
    from md5 import md5

__all__ = ['CloudFilesFS']

class ProxyConnection(Connection):
    """
    Add X-Forwarded-For header to all requests.
    """
    def __init__(self, *args, **kwargs):
        self.real_ip = None
        super(ProxyConnection, self).__init__(*args, **kwargs)

    def make_request(self, method, path=[], data='', hdrs=None, parms=None):
        if self.real_ip:
            if not hdrs:
                hdrs = {}
            hdrs['X-Forwarded-For'] = self.real_ip
        return super(ProxyConnection, self).make_request(method, path, data, hdrs, parms)

def translate_cloudfiles_error(fn):
    """
    Decorator to catch cloudfiles errors and translating them into IOSError.

    Other exceptions are not caught.
    """
    @wraps(fn)
    def wrapper(*args,**kwargs):
        name = getattr(fn, "func_name", "unknown")
        log = lambda msg: logging.warning("%s: %s" % (name, msg))
        try:
            return fn(*args, **kwargs)
        except (cloudfiles.errors.NoSuchContainer,
                cloudfiles.errors.NoSuchObject), e:
            raise IOSError(ENOENT, 'Not found: %s' % e)
        except cloudfiles.errors.ContainerNotEmpty, e:
            raise IOSError(ENOTEMPTY, 'Directory not empty: %s' % e)
        except cloudfiles.errors.ResponseError, e:
            log("Response error: %s" % e)
            # FIXME make some attempt to raise different errors on e.status
            raise IOSError(EPERM, 'Operation not permitted: %s' % e)
        except (cloudfiles.errors.AuthenticationError,
                cloudfiles.errors.AuthenticationFailed,
                cloudfiles.errors.ContainerNotPublic), e:
            log("Authentication error: %s" % e)
            raise IOSError(EPERM, 'Operation not permitted: %s' % e)
        except (cloudfiles.errors.CDNNotEnabled,
                cloudfiles.errors.IncompleteSend,
                cloudfiles.errors.InvalidContainerName,
                cloudfiles.errors.InvalidMetaName,
                cloudfiles.errors.InvalidMetaValue,
                cloudfiles.errors.InvalidObjectName,
                cloudfiles.errors.InvalidObjectSize,
                cloudfiles.errors.InvalidUrl), e:
            log("Unexpected cloudfiles error: %s" % e)
            raise IOSError(EIO, 'Unexpected cloudfiles error')
    return wrapper

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
        self.stream = None

        if not all([container, obj]):
            self.closed = True
            raise IOSError(EPERM, 'Container and object requred')

        self.container = self.cffs._get_container(self.container)

        if 'r' in self.mode:
            self.obj = self.container.get_object(self.name)
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
        '''Read data from the object.

        We can use just one request because 'seek' is not supported.
        
        NB: It uses the size passed into the first call for all subsequent calls'''
        if not self.stream:
            self.stream = self.obj.stream(size)

        logging.debug("read size=%r, total_size=%r, obj.size=%r" % (size, self.total_size, self.obj.size))
        try:
            buff = self.stream.next()
            self.total_size += len(buff)
        except StopIteration:
            return ""
        else:
            return buff

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
        self.cache = {}
        self.when = time.time()
        self.memcache = None

        if self.cffs.memcache_hosts:
            logging.debug("connecting to memcache %r" % self.cffs.memcache_hosts)
            self.memcache = memcache.Client(self.cffs.memcache_hosts)

    def key(self, index):
        '''Returns a key for a user distributed cache'''
        logging.debug("cache key for %r" % [self.cffs.authurl, self.cffs.username, index])
        if not hasattr(self, "_key_base"):
            self._key_base = md5("%s%s" % (self.cffs.authurl, self.cffs.username)).hexdigest()
        return "%s-%s" % (self._key_base, md5(index).hexdigest())

    def flush(self):
        '''Flush the listdir cache'''
        if self.memcache and self.path is not None:
            logging.debug("flushing memcache for %r" % self.path)
            self.memcache.delete(self.key(self.path))
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
        try:
            objects = self.cffs.connection.list_containers_info()
        except cloudfiles.errors.ResponseError:
            # when implementing contaniners' ACL, getting the containers
            # list can raise a ResponseError, but still access to the
            # the containers we have permissions to access to
            return
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
        cache = None
        if self.memcache:
            cache = self.memcache.get(self.key(path))
            if cache:
                logging.debug("memcache hit %r" % self.key(path))
        if not cache:
            cache = {}
            if path == "":
                self.listdir_root(cache)
            else:
                container, obj = parse_fspath(path)
                self.listdir_container(cache, container, obj)
            if self.memcache:
                self.memcache.set(self.key(path), cache, self.MAX_CACHE_TIME)
                logging.debug("memcache stored %r" % self.key(path))
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
        if self.memcache:
            cache = self.memcache.get(self.key(path))
            if cache:
                logging.debug("memcache hit %r" % self.key(path))
                self.cache = cache
                self.path = path
                return True
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
            logging.debug("invalid cache for %r (path: %r)" % (directory, self.path))
            self.listdir(directory)
        if path != "":
            try:
                stat_info = self.cache[leaf]
            except KeyError:
                logging.debug("Didn't find %r in directory listing" % leaf)
                # it can be a container and the user doesn't have
                # permissions to list the root
                if directory == '/' and leaf:
                    try:
                        container = self.cffs.connection.get_container(leaf)
                    except cloudfiles.errors.ResponseError:
                        raise IOSError(ENOENT, 'No such file or directory %s' % leaf)

                    logging.debug("Accessing %r container without root listing" % leaf)
                    stat_info = self._make_stat(count=container.object_count, bytes=container.size_used)
                else:
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
    single_cache = True
    memcache_hosts = None

    @translate_cloudfiles_error
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

    @translate_cloudfiles_error
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
        self.connection = ProxyConnection(username, api_key, timeout=cloudfiles_api_timeout, **kwargs)

    def close(self):
        '''Dummy function which does nothing - no need to close'''
        pass

    @translate_cloudfiles_error
    def _get_container(self, container):
        '''Gets the named container returning a container object, raising
        IOSError if not found'''
        return self.connection.get_container(container)

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

    def mkstemp(self, suffix='', prefix='', dir=None, mode='wb'):
        """A wrapper around tempfile.mkstemp creating a file with a unique
        name.  Unlike mkstemp it returns an object with a file-like
        interface.
        """
        e = "mkstemp suffix=%r prefix=%r, dir=%r mode=%r - not implemented" % (suffix, prefix, dir, mode)
        logging.debug(e)
        raise IOSError(EPERM, 'Operation not permitted: %s' % e)

    @translate_cloudfiles_error
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

    @translate_cloudfiles_error
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
        self._listdir_cache.flush()

    @translate_cloudfiles_error
    def listdir(self, path):
        '''List a directory, raise OSError on error'''
        path = self.abspath(path)
        logging.debug("listdir %r" % path)
        return self._listdir_cache.listdir(path)

    @translate_cloudfiles_error
    def listdir_with_stat(self, path):
        '''Return the directory list of the path with stat objects for
        each, filling the cache in the process, as a list of tuples
        (leafname, stat_result)'''
        path = self.abspath(path)
        logging.debug("listdir_with_stat %r" % path)
        return self._listdir_cache.listdir_with_stat(path)

    @translate_cloudfiles_error
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
            cnt.delete_object(obj)
        else:
            logging.debug("Removing container %r" % (container,))
            self.connection.delete_container(container)
        self._listdir_cache.flush()

    @translate_cloudfiles_error
    def remove(self, path):
        '''Remove a file, raise OSError on error'''
        path = self.abspath(path)
        logging.debug("remove %r" % path)
        self._listdir_cache.flush()
        container, name = parse_fspath(path)

        if not name:
            raise IOSError(EACCES, "Can't remove a container")

        if self.isdir(path):
            raise IOSError(EACCES, "Can't remove a directory (use rmdir instead)")

        container = self._get_container(container)
        obj = container.get_object(name)
        container.delete_object(obj)
        self._listdir_cache.flush()
        return not name

    @translate_cloudfiles_error
    def _rename_container(self, src_container_name, dst_container_name):
        '''Rename src_container_name into dst_container_name'''
        logging.debug("rename container %r -> %r" % (src_container_name, dst_container_name))
        # Delete the old container first, raising error if not empty
        self.connection.delete_container(src_container_name)
        self.connection.create_container(dst_container_name)
        self._listdir_cache.flush()

    @translate_cloudfiles_error
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
        src_obj = src_container.get_object(src_path)
        # Copy src -> dst
        src_obj.copy_to(dst_container_name, dst_path)
        # Delete dst
        src_container.delete_object(src_path)
        self._listdir_cache.flush()

    def chmod(self, path, mode):
        '''Change file/directory mode'''
        e = "chmod %03o %r - not implemented" % (mode, path)
        logging.debug(e)
        raise IOSError(EPERM, 'Operation not permitted: %s' % e)

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

    @translate_cloudfiles_error
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

    def flush(self):
        '''Flush caches'''
        if self._listdir_cache:
            self._listdir_cache.flush()

    def get_user_by_uid(self, uid):
        '''
        Return the username associated with user id.
        If this can't be determined return raw uid instead.
        '''
        return self.username

    def get_group_by_gid(self, gid):
        '''
        Return the groupname associated with group id.
        If this can't be determined return raw gid instead.
        On Windows just return "group".
        '''
        return self.username

    def readlink(self, path):
        '''
        Return a string representing the path to which a
        symbolic link points.

        We never return that we have a symlink in stat, so this should
        never be called
        '''
        e = "readlink %r - not implemented" % path
        logging.debug(e)
        raise IOSError(EPERM, 'Operation not permitted: %s' % e)

    @translate_cloudfiles_error
    def md5(self, path):
        '''Return the object MD5 for path, raise OSError on error'''
        path = self.abspath(path)
        logging.debug("md5 %r" % path)
        container, name = parse_fspath(path)

        if not name:
            raise IOSError(EACCES, "Can't return the MD5 of a container")

        if self.isdir(path):
            # this is only 100% accurate for virtual directories
            raise IOSError(EACCES, "Can't return the MD5 of a directory")

        container = self._get_container(container)
        obj = container.get_object(name)
        return obj.etag

