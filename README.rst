=================================================
FTP Interface to OpenStack Object Storage (Swift)
=================================================

:Homepage:  https://pypi.python.org/pypi/ftp-cloudfs/
:Credits:   Copyright 2009--2013 Chmouel Boudjnah <chmouel@chmouel.com>
:Licence:   MIT


DESCRIPTION
===========

ftp-cloudfs is a ftp server acting as a proxy to `OpenStack Object Storage (swift)`_.
It allow you to connect via any FTP client to do upload/download or create containers.

By default the server will bind to port 2021 which allow to be run as a non
root/administrator user.

.. _OpenStack Object Storage (Swift): http://launchpad.net/swift

It supports pseudo-hierarchical folders/directories as described in the `OpenStack Object Storage API`_.

.. _OpenStack Object Storage API: http://docs.openstack.org/openstack-object-storage/developer/content/


REQUIREMENTS
============

- Python 2 >= 2.6
- python-swiftclient >= 1.6.0 - https://github.com/openstack/python-swiftclient/
- pyftpdlib >= 1.2.0 - http://code.google.com/p/pyftpdlib/
- python-daemon >= 1.5.5 - http://pypi.python.org/pypi/python-daemon/
- python-memcache >= 1.45 - http://www.tummy.com/Community/software/python-memcached/

IMPORTANT: pyftpdlib 1.2.0 has a couple of known issues (memory leak, file descriptor leak) and it shouldn't
be used in production systems. There's no ETA for the next release so meanwhile you can upgrade to a patched
version with pip. Just upgrade pip to the latest version and then run::

  pip install --upgrade -e svn+http://pyftpdlib.googlecode.com/svn/trunk@1230#egg=pyftpdlib


Operating Systems
=================

fpt-cloudfs is developed and tested in Ubuntu and Debian Linux distributions but it should work on any
Unix-like (including Mac OS X) as long as you install the requirements listed above.


INSTALL
=======

Use standard setup.py directives ie.::

  python setup.py install

Or if you have `pip`_ installed you can just run::

  pip install ftp-cloudfs

which will install ftp-cloudfs with all the dependencies needed.

ftp-cloudfs has been `included in Debian Jessie`_.

.. _`pip`: http://pip.openplans.org/
.. _included in Debian Jessie: http://packages.debian.org/jessie/ftp-cloudfs


USAGE
======

The install should have created a /usr/bin/ftpcloudfs (or whatever
prefix defined in your python distribution or command line arguments)
which can be used like this:

Usage: ftpcloudfs [options]

Options:
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -p PORT, --port=PORT  Port to bind the server (default: 2021)
  -b BIND_ADDRESS, --bind-address=BIND_ADDRESS
                        Address to bind (default: 127.0.0.1)
  -a AUTHURL, --auth-url=AUTHURL
                        Authentication URL (required)
  --memcache=MEMCACHE   Memcache server(s) to be used for cache (ip:port)
  -v, --verbose         Be verbose on logging
  -f, --foreground      Do not attempt to daemonize but run in foreground
  -l LOG_FILE, --log-file=LOG_FILE
                        Log File: Default stdout when in foreground
  --syslog              Enable logging to the system logger (daemon facility)
  --pid-file=PID_FILE   Pid file location when in daemon mode
  --uid=UID             UID to drop the privilige to when in daemon mode
  --gid=GID             GID to drop the privilige to when in daemon mode
  --keystone-auth       Use auth 2.0 (Keystone, requires keystoneclient)
  --keystone-region-name=REGION_NAME
                        Region name to be used in auth 2.0
  --keystone-tenant-separator=TENANT_SEPARATOR
                        Character used to separate tenant_name/username in
                        auth 2.0 (default: TENANT.USERNAME)
  --keystone-service-type=SERVICE_TYPE
                        Service type to be used in auth 2.0 (default: object-
                        store)
  --keystone-endpoint-type=ENDPOINT_TYPE
                        Endpoint type to be used in auth 2.0 (default:
                        publicURL)

The defaults can be changed using a configuration file (by default in
/etc/ftpcloudfs.conf). Check the example file included in the package.


CACHE MANAGEMENT
================

`OpenStack Object Storage (Swift)`_ is an object storage and not a real file system. 
This proxy simulates enough file system functionality to be used over FTP, but it
has a performance impact.

To improve the performance a cache is used. It can be local or external (with
Memcache). By default a local cache is used, unless one or more Memcache servers
are configured.

If you're using just one client the local cache may be fine, but if you're using
several connections, configuring an external cache is highly recommended.


AUTH 2.0
========

By default ftp-cloudfs will use Swift auth 1.0, that is compatible with `OpenStack Object Storage`
using `swauth`_ auth middleware and Swift implementations such as `Rackspace Cloud Files` or
`Memset's Memstore Cloud Storage`.

Optionally `OpenStack Identity Service 2.0`_ can be used. Currently python-keystoneclient (0.3.2+
recommended) is required to use auth 2.0 and it can be enabled with ``keystone-auth`` option.

You can provide a tenant name in the FTP login user with TENANT.USERNAME (using a dot as
separator). Please check the example configuration file for further details.

.. _swauth: https://github.com/gholt/swauth
.. _OpenStack Identity Service 2.0: http://docs.openstack.org/api/openstack-identity-service/2.0/content/index.html
.. _RackSpace Cloud Files: http://www.rackspace.com/cloud/cloud_hosting_products/files/
.. _Memset's Memstore Cloud Storage: https://www.memset.com/cloud/storage/


SUPPORT
=======

The project website is at:

https://github.com/cloudfs/ftp-cloudfs/issues

There you can file bug reports, ask for help or contribute patches. There's additional information at:

https://github.com/cloudfs/ftp-cloudfs/wiki

LICENSE
=======

Unless otherwise noted, all files are released under the `MIT`_ license,
exceptions contain licensing information in them.

.. _`MIT`: http://en.wikipedia.org/wiki/MIT_License

  Copyright (C) 2009-2013 Chmouel Boudjnah <chmouel@chmouel.com>

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.


Authors
=======

- Chmouel Boudjnah <chmouel@chmouel.com>
- Nick Craig-Wood <nick@craig-wood.com>
- Juan J. Martinez <jjm@usebox.net>


Contributors
============

- Christophe Le Guern <c35sys@gmail.com>
- Konstantin vz'One Enchant <sirkonst@gmail.com>
- Maxim Mitroshin <mitroshin@selectel.org>

