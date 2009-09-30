#!/usr/bin/env python

from distutils.core import setup
from ftpcloudfs.constants import version

setup(name='ftp-cloudfs',
      version=version,
      description='FTP interface to Rackspace Cloud Files',
      author='Chmouel Boudjnah',
      author_email='chmouel.boudjnah@rackspace.co.uk',
      url='http://blog.chmouel.com',
      packages=['ftpcloudfs'],
      scripts=['scripts/ftpcloudfs']
     )
