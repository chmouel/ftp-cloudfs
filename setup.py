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
      description='FTP interface to OpenStack Object Storage (Swift)',
      author='Chmouel Boudjnah',
      author_email='chmouel@chmouel.com',
      url='https://pypi.python.org/pypi/ftp-cloudfs/',
      long_description = read('README.rst'),
      license='MIT',
      include_package_data=True,
      zip_safe=False,
      install_requires=['pyftpdlib>=1.3.0', 'python-swiftclient>=1.6.0', 'python-daemon>=1.5.5', 'python-memcached'],
      scripts=['bin/ftpcloudfs'],
      packages = find_packages(exclude=['tests',]),
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
