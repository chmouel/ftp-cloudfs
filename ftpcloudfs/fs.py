"""
    A filesystem like interface to an object storage.

Authors: Chmouel Boudjnah <chmouel@chmouel.com>
         Nick Craig-Wood <nick@craig-wood.com>
         Juan J. Martinez <jjm@usebox.net>
"""

import os
import sys
import time
import mimetypes
import stat
import logging
from errno import EPERM, ENOENT, EACCES, EIO, ENOTDIR, ENOTEMPTY
from swiftclient.client import Connection, ClientException
from chunkobject import ChunkObject
from errors import IOSError
import posixpath
from utils import smart_str
from functools import wraps
import memcache
import multiprocessing
try:
    from hashlib import md5
except ImportError:
    from md5 import md5
try:
    import json
except ImportError:
    import simplejson as json

__all__ = ['ObjectStorageFS']

class ProxyConnection(Connection):
    """
    Add X-Forwarded-For header to all requests.

    Optionally if `range_from` is available it will be used to add a Range header
    starting from it.
    """

    # max time to cache auth tokens (seconds), based on swift defaults
    TOKEN_TTL = 86400

    def __init__(self, memcache, *args, **kwargs):
        self.memcache = memcache
        self.real_ip = None
        self.range_from = None
        self.ignore_auth_cache = False
        super(ProxyConnection, self).__init__(*args, **kwargs)

    def http_connection(self):
        def request_wrapper(fn):
            @wraps(fn)
            def request_x_forwarded_for(method, url, body=None, headers=None):
                if headers is None:
                    headers = {}
                if self.real_ip:
                    headers['X-Forwarded-For'] = self.real_ip
                if self.range_from:
                    headers['Range'] = "bytes=%s-" % self.range_from
                    # only for one request
                    self.range_from = None

                fn(method, url, body=body, headers=headers)
            return request_x_forwarded_for

        parsed, conn = super(ProxyConnection, self).http_connection()
        conn.request = request_wrapper(conn.request)

        return parsed, conn

    def get_auth(self):
        """Perform the authentication using a token cache if memcache is available"""
        if self.memcache:
            key = "tk%s" % md5("%s%s%s" % (self.authurl, self.user, self.key)).hexdigest()
            cache = self.memcache.get(key)
            if not cache or self.ignore_auth_cache:
                logging.debug("token cache miss, key=%s" % key)
                cache = super(ProxyConnection, self).get_auth()
                self.memcache.set(key, cache, self.TOKEN_TTL)
                self.ignore_auth_cache = False
            else:
                logging.debug("token cache hit, key=%s" % key)
                self.ignore_auth_cache = True
            return cache
        # no memcache
        return super(ProxyConnection, self).get_auth()

def translate_objectstorage_error(fn):
    """
    Decorator to catch Object Storage errors and translating them into IOSError.

    Other exceptions are not caught.
    """
    @wraps(fn)
    def wrapper(*args,**kwargs):
        name = getattr(fn, "func_name", "unknown")
        log = lambda msg: logging.debug("At %s: %s" % (name, msg))
        try:
            return fn(*args, **kwargs)
        except ClientException, e:
            # some errno mapping
            if e.http_status == 404:
                err = ENOENT
            elif e.http_status == 400:
                err = EPERM
            elif e.http_status == 403:
                err = EACCES
            else:
                err = EIO

            msg = "%s: %s" % (smart_str(e.msg), smart_str(e.http_reason))
            log(msg)
            raise IOSError(err, msg)
    return wrapper

def parse_fspath(path):
    """
    Returns a (container, path) tuple.

    For shorter paths replaces not provided values with empty strings.
    May raise IOSError for invalid paths.
    """
    if not path.startswith('/'):
        logging.warning('parse_fspath: You have to provide an absolute path: %r' % path)
        raise IOSError(ENOENT, 'Absolute path needed')
    parts = path.split('/', 2)[1:]
    while len(parts) < 2:
        parts.append('')
    return tuple(parts)

class ObjectStorageFD(object):
    """File alike object attached to the Object Storage."""

    split_size = 0

    def __init__(self, cffs, container, obj, mode):
        self.cffs = cffs
        self.container = container
        self.name = obj
        self.mode = mode
        self.closed = False
        self.total_size = 0
        self.part_size = 0
        self.part = 0
        self.headers = dict()
        self.content_type = mimetypes.guess_type(self.name)[0]
        self.pending_copy_task = None

        self.obj = None

        # this is only used by `seek`, so we delay the HEAD request until is required
        self.size = None

        if not all([container, obj]):
            self.closed = True
            raise IOSError(EPERM, 'Container and object required')

        logging.debug("ObjectStorageFD object: %r (mode: %r)" % (obj, mode))

        if 'r' in self.mode:
            logging.debug("read fd %r" % self.name)
        else: # write
            logging.debug("write fd %r" % self.name)
            self.obj = ChunkObject(self.conn, self.container, self.name, content_type=self.content_type)

    @property
    def part_base_name(self):
        return u"%s.part" % self.name

    @property
    def part_name(self):
        return u"%s/%.6d" % (self.part_base_name, self.part)

    @property
    def conn(self):
        """Connection to the storage."""
        return self.cffs.conn

    def _start_copy_task(self):
        """
        Copy the first part of a multi-part file to its final location and create
        the manifest file.

        This happens in the background, pending_copy_task must be cleaned up at
        the end.
        """
        def copy_task(conn, container, name, part_name, part_base_name):
            # open a new connection
            conn = ProxyConnection(None, preauthurl=conn.url, preauthtoken=conn.token)
            headers = { 'x-copy-from': "/%s/%s" % (container, name) }
            logging.debug("copying first part %r/%r, %r" % (container, part_name, headers))
            try:
                conn.put_object(container, part_name, headers=headers, contents=None)
            except ClientException as ex:
                logging.error("Failed to copy %s: %s" % (name, ex.http_reason))
                sys.exit(1)
            # setup the manifest
            headers = { 'x-object-manifest': "%s/%s" % (container, part_base_name) }
            logging.debug("creating manifest %r/%r, %r" % (container, name, headers))
            try:
                conn.put_object(container, name, headers=headers, contents=None)
            except ClientException as ex:
                logging.error("Failed to store the manifest %s: %s" % (name, ex.http_reason))
                sys.exit(1)
            logging.debug("copy task done")
        self.pending_copy_task = multiprocessing.Process(target=copy_task,
                                                         args=(self.conn,
                                                               self.container,
                                                               self.name,
                                                               self.part_name,
                                                               self.part_base_name,
                                                               ),
                                                         )
        self.pending_copy_task.start()

    @translate_objectstorage_error
    def write(self, data):
        """Write data to the object."""
        if 'r' in self.mode:
            raise IOSError(EPERM, "File is opened for read")

        # large file support
        if self.split_size:
            # data can be of any size, so we need to split it in split_size chunks
            offs = 0
            while offs < len(data):
                if self.part_size + len(data) - offs > self.split_size:
                    current_size = self.split_size-self.part_size
                    logging.debug("data is to large (%r), using %s" % (len(data), current_size))
                else:
                    current_size = len(data)-offs
                self.part_size += current_size
                if not self.obj:
                    self.obj = ChunkObject(self.conn, self.container, self.part_name, content_type=self.content_type)
                self.obj.send_chunk(data[offs:offs+current_size])
                offs += current_size
                if self.part_size == self.split_size:
                    logging.debug("current size is %r, split_file is %r" % (self.part_size, self.split_size))
                    self.obj.finish_chunk()
                    # this obj is not valid anymore, will create a new one if a new part is required
                    self.obj = None
                    # make it the first part
                    if self.part == 0:
                        self._start_copy_task()
                    self.part_size = 0
                    self.part += 1
        else:
            self.obj.send_chunk(data)

    @translate_objectstorage_error
    def close(self):
        """Close the object and finish the data transfer."""
        if 'r' in self.mode:
            return
        if self.pending_copy_task:
            logging.debug("waiting for a pending copy task...")
            self.pending_copy_task.join()
            logging.debug("wait is over")
            if self.pending_copy_task.exitcode != 0:
                raise IOSError(EIO, 'Failed to store the file')
        if self.obj is not None:
            self.obj.finish_chunk()

    def read(self, size=65536):
        """
        Read data from the object.

        We can use just one request because 'seek' is not fully supported.

        NB: It uses the size passed into the first call for all subsequent calls.
        """
        if self.obj is None:
            if self.total_size > 0:
                self.conn.range_from = self.total_size
                # we need to open a new connection to inject the `Range` header
                if self.conn.http_conn:
                    self.conn.http_conn[1].close()
                    self.conn.http_conn = None
            _, self.obj = self.conn.get_object(self.container, self.name, resp_chunk_size=size)

        logging.debug("read size=%r, total_size=%r (range_from: %s)" % (size,
                self.total_size, self.conn.range_from))

        try:
            buff = self.obj.next()
            self.total_size += len(buff)
        except StopIteration:
            return ""
        else:
            return buff

    def seek(self, offset, whence=None):
        """
        Seek in the object.

        It's supported only for read operations because of object storage limitations.
        """
        logging.debug("seek offset=%s, whence=%s" % (str(offset), str(whence)))

        if 'r' in self.mode:

            if self.size is None:
                meta = self.conn.head_object(self.container, self.name)
                try:
                    self.size = int(meta["content-length"])
                except ValueError:
                    raise IOSError(EPERM, "Invalid file size")

            if not whence:
                offs = offset
            elif whence == 1:
                offs = self.total_size + offset
            elif whence == 2:
                offs = self.size - offset
            else:
                raise IOSError(EPERM, "Invalid file offset")

            if offs < 0 or offs > self.size:
                raise IOSError(EPERM, "Invalid file offset")

            # we need to start over after a seek call
            if self.obj is not None:
                del self.obj # GC the generator
                self.obj = None
            self.total_size = offs
        else:
            raise IOSError(EPERM, "Seek not available for write operations")

class CacheEncoder(json.JSONEncoder):
    """JSONEncoder to encode the os.stat_result values into a list."""
    def default(self, obj):
        if isinstance(obj, os.stat_result):
            return tuple(obj)
        return json.JSONEncoder.default(self, obj)

def serialize(obj):
    """Serialize a cache dict into a JSON object."""
    return json.dumps(obj, cls=CacheEncoder)

def unserialize(js):
    """Unserialize a JSON object into a cache dict."""
    return dict(((smart_str(key), os.stat_result(value)) for key, value in json.loads(js).iteritems()))

class ListDirCache(object):
    """
    Cache for listdir.

    This is to cache the very common case when we call listdir and then
    immediately call stat() on all the objects.

    In the OS this would be cached in the VFS but we have to make our
    own caching here to avoid the stat calls each making a connection.
    """
    MAX_CACHE_TIME = 10         # seconds to cache the listdir for
    MIN_COMPRESS_LEN = 4096     # min length in bytes to compress cache entries
    memcache = None

    def __init__(self, cffs):
        self.cffs = cffs
        self.path = None
        self.cache = {}
        self.when = time.time()

        if self.cffs.memcache_hosts and ListDirCache.memcache is None:
            logging.debug("connecting to memcache %r" % self.cffs.memcache_hosts)
            ListDirCache.memcache = memcache.Client(self.cffs.memcache_hosts)

    @property
    def conn(self):
        """Connection to the storage."""
        return self.cffs.conn

    def key(self, index):
        """Returns a key for a user distributed cache."""
        logging.debug("cache key for %r" % [self.cffs.authurl, self.cffs.username, index])
        if not hasattr(self, "_key_base"):
            self._key_base = md5("%s%s" % (self.cffs.authurl, self.cffs.username)).hexdigest()
        return "%s-%s" % (self._key_base, md5(smart_str(index)).hexdigest())

    def flush(self, path=None):
        """Flush the listdir cache."""
        logging.debug("cache flush, current path: %s request: %s" % (self.path, path))
        if self.memcache:
            if path is not None:
                logging.debug("flushing memcache for %r" % path)
                self.memcache.delete(self.key(path))
                if self.path == path:
                    self.cache = None
            elif self.path is not None:
                logging.debug("flushing memcache for %r" % self.path)
                self.memcache.delete(self.key(path))
                self.cache = None
        else:
            self.cache = None

    def _make_stat(self, last_modified=None, content_type="application/directory", count=1, bytes=0, **kwargs):
        """Make a stat object from the parameters passed in from"""
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
        """Fills cache with the list dir of the container"""
        container = smart_str(container)
        path = smart_str(path)
        logging.debug("listdir container %r path %r" % (container, path))
        if path:
            prefix = path.rstrip("/")+"/"
        else:
            prefix = None
        _, objects = self.conn.get_container(container, prefix=prefix, delimiter="/")

        # override 10000 objects limit with markers
        nbobjects = len(objects)
        while nbobjects >= 10000:
            # get last object as a marker
            lastobject = objects[-1]
            if 'subdir' in lastobject:
                # {u'subdir': 'dirname'}
                lastobjectname = lastobject['subdir'].rstrip("/")
            else:
                lastobjectname = lastobject['name']
            # get a new list with the marker
            _, newobjects = self.conn.get_container(container, prefix=prefix, delimiter="/", marker=lastobjectname)
            # get the new list length
            nbobjects = len(newobjects)
            logging.debug("number of objects after marker %s: %s" % (lastobjectname, nbobjects))
            # add the new list to current list
            objects.extend(newobjects)
        logging.debug("total number of objects %s:" % len(objects))

        for obj in objects:
            # {u'bytes': 4820,  u'content_type': '...',  u'hash': u'...',  u'last_modified': u'2008-11-05T00:56:00.406565',  u'name': u'new_object'},
            if 'subdir' in obj:
                # {u'subdir': 'dirname'}
                obj['name'] = obj['subdir'].rstrip("/")
            elif obj.get('bytes') == 0 and obj.get('hash') and obj.get('content_type') != 'application/directory':
                # if it's a 0 byte file, has a hash and is not a directory, we make an extra call
                # to check if it's a manifest file and retrieve the real size / hash
                manifest_obj = self.conn.head_object(container, obj['name'])
                logging.debug("possible manifest file: %r" % manifest_obj)
                if 'x-object-manifest' in manifest_obj:
                    logging.debug("manifest found: %s" % manifest_obj['x-object-manifest'])
                    obj['hash'] = manifest_obj['etag']
                    obj['bytes'] = int(manifest_obj['content-length'])
            obj['count'] = 1
            # Keep all names in utf-8, just like the filesystem
            name = posixpath.basename(obj['name']).encode("utf-8")
            cache[name] = self._make_stat(**obj)

    def listdir_root(self, cache):
        """Fills cache with the list of containers"""
        logging.debug("listdir root")
        try:
            _, objects = self.conn.get_account()
        except ClientException:
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
        """Return the directory list of the path, filling the cache in the process"""
        path = path.rstrip("/") or "/"
        logging.debug("listdir %r" % path)
        cache = None
        if self.memcache:
            cache = self.memcache.get(self.key(path))
            if cache:
                cache = unserialize(cache)
                logging.debug("memcache hit %r" % self.key(path))
            else:
                logging.debug("memcache miss %r" % self.key(path))
        if not cache:
            cache = {}
            if path == "/":
                self.listdir_root(cache)
            else:
                container, obj = parse_fspath(path)
                self.listdir_container(cache, container, obj)
            if self.memcache:
                if self.memcache.set(self.key(path), serialize(cache), self.MAX_CACHE_TIME, min_compress_len=self.MIN_COMPRESS_LEN):
                    logging.debug("memcache stored %r" % self.key(path))
                else:
                    logging.warning("Failed to store the cache")
        self.cache = cache
        self.path = path
        self.when = time.time()
        leaves = sorted(self.cache.keys())
        logging.debug(".. %r" % leaves)
        return leaves

    def listdir_with_stat(self, path):
        """
        Return the directory list of the path with stat objects.

        The cache will be filled in in the process, as a list of tuples
        (leafname, stat_result).
        """
        self.listdir(path)
        return sorted(self.cache.iteritems())

    def valid(self, path):
        """Check the cache is valid for the container and directory path"""
        if not self.cache or self.path != path:
            if self.memcache:
                cache = self.memcache.get(self.key(path))
                if cache:
                    cache = unserialize(cache)
                    logging.debug("memcache hit %r" % self.key(path))
                    self.cache = cache
                    self.path = path
                    return True
            return False
        age = time.time() - self.when
        return age < self.MAX_CACHE_TIME

    def stat(self, path):
        """
        Returns an os.stat_result for path or raises IOSError.

        Returns the information from the cache if possible.
        """
        path = path.rstrip("/") or "/"
        logging.debug("stat path %r" % (path))
        directory, leaf = posixpath.split(path)
        # Refresh the cache it if is old, or wrong
        if not self.valid(directory):
            logging.debug("invalid cache for %r (path: %r)" % (directory, self.path))
            self.listdir(directory)
        if path != "/":
            try:
                stat_info = self.cache[smart_str(leaf)]
            except KeyError:
                logging.debug("Didn't find %r in directory listing" % leaf)
                # it can be a container and the user doesn't have
                # permissions to list the root
                if directory == '/' and leaf:
                    try:
                        container = self.conn.head_container(leaf)
                    except ClientException:
                        raise IOSError(ENOENT, 'No such file or directory %s' % leaf)

                    logging.debug("Accessing %r container without root listing" % leaf)
                    stat_info = self._make_stat(count=int(container["x-container-object-count"]),
                                                bytes=int(container["x-container-bytes-used"]),
                                                )
                else:
                    raise IOSError(ENOENT, 'No such file or directory %s' % leaf)
        else:
            # Root directory size is sum of containers, count is containers
            bytes = sum(stat_info.st_size for stat_info in self.cache.values())
            count = len(self.cache)
            stat_info = self._make_stat(count=count, bytes=bytes)
        logging.debug("stat path: %r" % stat_info)
        return stat_info

class ObjectStorageFS(object):
    """
    Object Storage File System emulation.

    All the methods on this class emulate os.* or os.path.* functions
    of the same name.
    """
    memcache_hosts = None

    @translate_objectstorage_error
    def __init__(self, username, api_key, authurl, keystone=None):
        """
        Create the Object Storage connection.

        username - if None then don't make the connection
        api_key
        authurl
        keystone - optional for auth 2.0 (keystone)
        """
        self.username = username
        self.conn = None
        self.authurl = authurl
        self.keystone = keystone
        # A cache to hold the information from the last listdir
        self._listdir_cache = ListDirCache(self)
        self._cwd = '/'
        if username is not None:
            self.authenticate(username, api_key)

    @translate_objectstorage_error
    def authenticate(self, username, api_key):
        """Authenticates and opens the connection"""
        if not username or not api_key:
            raise ClientException("username/password required", http_status=401)

        kwargs = dict(authurl=self.authurl, auth_version="1.0")

        if self.keystone:
            if self.keystone['tenant_separator'] in username:
                tenant_name, username = username.split(self.keystone['tenant_separator'], 1)
            else:
                tenant_name = None

            logging.debug("keystone authurl=%r username=%r tenant_name=%r conf=%r" % (self.authurl, username, tenant_name, self.keystone))

            ks = self.keystone
            kwargs["auth_version"] = "2.0"
            kwargs["tenant_name"] = tenant_name
            kwargs["os_options"] = dict(service_type=ks['service_type'],
                                        endpoint_type=ks['endpoint_type'],
                                        region_name=ks['region_name'],
                                        )

        self.conn = ProxyConnection(self._listdir_cache.memcache,
                                    user=username,
                                    key=api_key,
                                    **kwargs
                                    )
        # force authentication
        self.conn.url, self.conn.token = self.conn.get_auth()
        self.conn.http_conn = None

    def close(self):
        """Dummy function which does nothing - no need to close"""
        pass

    def isabs(self, path):
        """Test whether a path is absolute"""
        return posixpath.isabs(path)

    def normpath(self, path):
        """Normalize path, eliminating double slashes, etc"""
        return posixpath.normpath(path)

    def abspath(self, path):
        """Return an absolute path"""
        if not self.isabs(path):
            path = posixpath.join(self.getcwd(), path)
        return self.normpath(path)

    def mkstemp(self, suffix='', prefix='', dir=None, mode='wb'):
        """
        A wrapper around tempfile.mkstemp creating a file with a unique name.

        Unlike mkstemp it returns an object with a file-like interface.
        """
        e = "mkstemp suffix=%r prefix=%r, dir=%r mode=%r - not implemented" % (suffix, prefix, dir, mode)
        logging.debug(e)
        raise IOSError(EPERM, 'Operation not permitted: %s' % e)

    @translate_objectstorage_error
    def open(self, path, mode):
        """Open path with mode, raise IOError on error"""
        path = self.abspath(path)
        logging.debug("open %r mode %r" % (path, mode))
        self._listdir_cache.flush(posixpath.dirname(path))
        container, obj = parse_fspath(path)
        return ObjectStorageFD(self, container, obj, mode)

    def chdir(self, path):
        """Change current directory, raise OSError on error"""
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
        """Returns the current working directory"""
        return self._cwd

    def _container_exists(self, container):
        # verify the container exsists
        try:
            self.conn.head_container(container)
        except ClientException, e:
            if e.http_status == 404:
                raise IOSError(ENOTDIR, "Container not found")
            raise
        return True

    @translate_objectstorage_error
    def mkdir(self, path):
        """
        Make a directory.

        Raises OSError on error.
        """
        path = self.abspath(path)
        logging.debug("mkdir %r" % path)
        container, obj = parse_fspath(path)
        if obj:
            self._listdir_cache.flush(posixpath.dirname(path))
            logging.debug("Making directory %r in %r" % (obj, container))
            self._container_exists(container)
            self.conn.put_object(container, obj, contents=None, content_type="application/directory")
        else:
            self._listdir_cache.flush("/")
            logging.debug("Making container %r" % (container,))
            self.conn.put_container(container)

    @translate_objectstorage_error
    def listdir(self, path):
        """
        List a directory.

        Raises OSError on error.
        """
        path = self.abspath(path)
        logging.debug("listdir %r" % path)
        list_dir = map(lambda x: unicode(x, 'utf-8'), self._listdir_cache.listdir(path))
        return list_dir

    @translate_objectstorage_error
    def listdir_with_stat(self, path):
        """
        Return the directory list of the path with stat objects.

        The the cache is filled in the process, as a list of tuples (leafname, stat_result).
        """
        path = self.abspath(path)
        logging.debug("listdir_with_stat %r" % path)
        return [(unicode(name, 'utf-8)'), stat) for name, stat in self._listdir_cache.listdir_with_stat(path)]

    @translate_objectstorage_error
    def rmdir(self, path):
        """
        Remove a directory.

        Eaise OSError on error.
        """
        path = self.abspath(path)
        logging.debug("rmdir %r" % path)
        container, obj = parse_fspath(path)

        if not self.isdir(path):
            if self.isfile(path):
                raise IOSError(ENOTDIR, "Not a directory")
            raise IOSError(ENOENT, 'No such file or directory')

        if self.listdir(path):
            raise IOSError(ENOTEMPTY, "Directory not empty: %s" % path)

        if obj:
            self._listdir_cache.flush(posixpath.dirname(path))
            logging.debug("Removing directory %r in %r" % (obj, container))
            self.conn.delete_object(container, obj)
        else:
            self._listdir_cache.flush("/")
            logging.debug("Removing container %r" % (container,))
            self.conn.delete_container(container)

    @translate_objectstorage_error
    def remove(self, path):
        """
        Remove a file.

        Raises OSError on error.
        """
        path = self.abspath(path)
        logging.debug("remove %r" % path)
        container, name = parse_fspath(path)

        if not name:
            raise IOSError(EACCES, "Can't remove a container")

        if self.isdir(path):
            raise IOSError(EACCES, "Can't remove a directory (use rmdir instead)")

        self.conn.delete_object(container, name)
        self._listdir_cache.flush(posixpath.dirname(path))
        return not name

    @translate_objectstorage_error
    def _rename_container(self, src_container_name, dst_container_name):
        """Rename src_container_name into dst_container_name"""
        logging.debug("rename container %r -> %r" % (src_container_name, dst_container_name))
        # Delete the old container first, raising error if not empty
        self.conn.delete_container(src_container_name)
        self.conn.put_container(dst_container_name)
        self._listdir_cache.flush("/")

    @translate_objectstorage_error
    def rename(self, src, dst):
        """
        Rename a file/directory from src to dst.

        Raises OSError on error.
        """
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
                raise IOSError(ENOTEMPTY, "Can't rename non-empty directory: %s" % src)
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
            raise IOSError(EACCES, "Can't rename to / from root")
        # Check destination directory exists
        if not self.isdir(posixpath.split(dst)[0]):
            raise IOSError(ENOENT, "Can't copy %r to %r, destination directory doesn't exist" % (src, dst))

        # check src/dst containers
        self._container_exists(src_container_name)
        self._container_exists(dst_container_name)

        # Do the rename of the file/dir
        headers = { 'x-copy-from': "/%s/%s" % (src_container_name, src_path) }
        self.conn.put_object(dst_container_name, dst_path, headers=headers, contents=None)
        # Delete src
        self.conn.delete_object(src_container_name, src_path)
        self._listdir_cache.flush(posixpath.dirname(src))
        self._listdir_cache.flush(posixpath.dirname(dst))

    def chmod(self, path, mode):
        """Change file/directory mode"""
        e = "chmod %03o %r - not implemented" % (mode, path)
        logging.debug(e)
        raise IOSError(EPERM, 'Operation not permitted: %s' % e)

    def isfile(self, path):
        """
        Is this path a file.

        Shouldn't raise an error if not found like os.path.isfile.
        """
        logging.debug("isfile %r" % path)
        try:
            return stat.S_ISREG(self.stat(path).st_mode)
        except EnvironmentError:
            return False

    def islink(self, path):
        """
        Is this path a link.

        Shouldn't raise an error if not found like os.path.islink.
        """
        logging.debug("islink %r" % path)
        return False

    def isdir(self, path):
        """
        Is this path a directory.

        Shouldn't raise an error if not found like os.path.isdir.
        """
        logging.debug("isdir %r" % path)
        try:
            return stat.S_ISDIR(self.stat(path).st_mode)
        except EnvironmentError:
            return False

    def getsize(self, path):
        """
        Return the size of path.

        Raises OSError on error.
        """
        logging.debug("getsize %r" % path)
        return self.stat(path).st_size

    def getmtime(self, path):
        """
        Return the modification time of path.

        Raises OSError on error.
        """
        logging.debug("getmtime %r" % path)
        return self.stat(path).st_mtime

    def realpath(self, path):
        """Return the canonical path of the specified path"""
        return self.abspath(path)

    def lexists(self, path):
        """
        Test whether a path exists.

        Returns True for broken symbolic links.
        """
        logging.debug("lexists %r" % path)
        try:
            self.stat(path)
            return True
        except EnvironmentError:
            return False

    @translate_objectstorage_error
    def stat(self, path):
        """
        Return os.stat_result object for path.

        Raises OSError on error.
        """
        path = self.abspath(path)
        logging.debug("stat %r" % path)
        return self._listdir_cache.stat(path)

    exists = lexists
    lstat = stat

    def validpath(self, path):
        """Check whether the path belongs to user's home directory"""
        return True

    def flush(self):
        """Flush cache"""
        if self._listdir_cache:
            self._listdir_cache.flush()

    def get_user_by_uid(self, uid):
        """
        Return the username associated with user id.

        If this can't be determined return raw uid instead.
        """
        return self.username

    def get_group_by_gid(self, gid):
        """
        Return the groupname associated with group id.

        If this can't be determined return raw gid instead.
        On Windows just return "group".
        """
        return self.username

    def readlink(self, path):
        """
        Return a string representing the path to which a symbolic link points.

        We never return that we have a symlink in stat, so this should
        never be called.
        """
        e = "readlink %r - not implemented" % path
        logging.debug(e)
        raise IOSError(EPERM, 'Operation not permitted: %s' % e)

    @translate_objectstorage_error
    def md5(self, path):
        """
        Return the object MD5 for path.

        Raise OSError on error.
        """
        path = self.abspath(path)
        logging.debug("md5 %r" % path)
        container, name = parse_fspath(path)

        if not name:
            raise IOSError(EACCES, "Can't return the MD5 of a container")

        if self.isdir(path):
            # this is only 100% accurate for virtual directories
            raise IOSError(EACCES, "Can't return the MD5 of a directory")

        meta = self.conn.head_object(container, name)
        return meta["etag"]

