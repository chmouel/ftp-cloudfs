==========================================================
FTP Interface to Rackspace Cloud Files and OpenStack Swift
==========================================================

:Homepage:  http://blog.chmouel.com/2009/10/29/ftp-server-for-cloud-files/
:Credits:   Copyright 2009--2011 Chmouel Boudjnah <chmouel@chmouel.com>
:Licence:   BSD


DESCRIPTION
===========

ftp-cloudfs is a ftp server acting as a proxy to `Rackspace Cloud Files`_ or to `OpenStack Swift`_. It allow you to connect via any FTP client to do
upload/download or create containers.

By default the server will bind to port 2021 which allow to be run as
a non root/administrator user.

.. _OpenStack Swift: http://launchpad.net/swift
.. _RackSpace Cloud Files: http://www.rackspace.com/cloud/cloud_hosting_products/files/

REQUIREMENT
===========

- Python >= 2.6 (probably 2.5 as well but not extensively tested)
- python-cloudfiles >= 1.3.0  - http://github.com/rackspace/python-cloudfiles
- pyftpdlib >= 0.6.0 - http://code.google.com/p/pyftpdlib/
- python-daemon >= 1.6 - http://pypi.python.org/pypi/python-daemon/

Operating Systems
=================

This has been tested on a Debian testing Linux distribution but it
should work on any Unices (including MacOSX) as long you have the
requirement listed above. 

It should as well work on Windows but this has been completely
untested.

INSTALL
=======

Use standard setup.py directives ie :

python setup.py install

Or if you have `pip`_ installed you can just do a ::

  pip install ftp-cloudfs

which will install ftp-cloudfs with all the dependences needed.

.. _`pip`: http://pip.openplans.org/

USAGE
======

The install should have created a /usr/bin/ftpcloudfs (or whatever
prefix defined in your python distribution or command line arguments)
which can be used like this :

Usage: ftpcloudfs [OPTIONS].....
  -h, --help            show this help message and exit
  -p PORT, --port=PORT  Port to bind the server default: 2021.
  -b BIND_ADDRESS, --bind-address=BIND_ADDRESS
                        Address to bind by default: 127.0.0.1.
  -a AUTHURL, --auth-url=AUTHURL
                        Auth URL for alternate providers(eg OpenStack)
  -s, --service-net     Connect via Rackspace ServiceNet network.
  -f, --foreground      Do not attempt to daemonize butrun in foreground.
  -l LOG_FILE, --log-file=LOG_FILE
                        Log File: Default stdout when in foreground
  --pid-file=PID_FILE   Pid file location when in daemon mode.
  --uid=UID             UID to drop the privilige to when in daemon mode
  --gid=GID             GID to drop the privilige to when in daemon mode

SUPPORT
=======

This tool is not supported by Rackspace in any sort but I will be
happy to help you as much as possible. Your best bet to report issues
and or feature request is to have them reported in the github issue
tracker :

https://github.com/chmouel/ftp-cloudfs/issues

LICENSE
=======

Unless otherwise noted, all files are released under the `MIT`_ license,
exceptions contain licensing information in them.

.. _`MIT`: http://en.wikipedia.org/wiki/MIT_License

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
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.

  Except as contained in this notice, the name of Rackspace US, Inc. shall not
  be used in advertising or otherwise to promote the sale, use or other dealings
  in this Software without prior written authorization from Rackspace US, Inc. 

Author
======

Chmouel Boudjnah <chmouel@chmouel.com>

Contributor
===========

Nick Craig-Wood <nick@craig-wood.com>
