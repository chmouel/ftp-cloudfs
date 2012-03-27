#!/usr/bin/env python
import os
from setuptools import setup, find_packages
from ftpcloudfs.constants import version

def read(fname):
    full_path = os.path.join(os.path.dirname(__file__), fname)
    if os.path.exists(fname):
        return open(full_path).read()
    else:
        return ""

setup(name='ftp-cloudfs',
      version=version,
      download_url="http://pypi.python.org/packages/source/f/ftp-cloudfs/ftp-cloudfs-%s.tar.gz" % (version),
      description='FTP interface to Rackspace Cloud Files and OpenStack Swift',
      author='Chmouel Boudjnah',
      author_email='chmouel.boudjnah@rackspace.co.uk',
      url='https://github.com/chmouel/ftp-cloudfs',
      long_description = read('README.rst'),
      license='MIT',
      include_package_data=True,
      zip_safe=False,
      install_requires=['pyftpdlib', 'python-cloudfiles','python-daemon', 'python-memcached'],
      scripts=['bin/ftpcloudfs'],
      packages = find_packages(exclude=['tests', 'debian']),
      tests_require = ["nose"],
      classifiers = [
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Programming Language :: Python',
        'Operating System :: OS Independent',
        'Environment :: No Input/Output (Daemon)',
        'License :: OSI Approved :: MIT License',
        ],
      test_suite = "nose.collector",
      )
