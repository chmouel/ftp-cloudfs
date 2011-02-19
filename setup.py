#!/usr/bin/env python
import os
from setuptools import setup, find_packages
from ftpcloudfs.constants import version

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(name='ftp-cloudfs',
      version=version,
      download_url="https://github.com/chmouel/ftp-cloudfs/zipball/%s" % (version),
      description='FTP interface to Rackspace Cloud Files and OpenStack Swift',
      author='Chmouel Boudjnah',
      author_email='chmouel.boudjnah@rackspace.co.uk',
      url='https://github.com/chmouel/ftp-cloudfs',
      long_description = read('README.rst'),
      license='MIT',
      include_package_data=True,
      zip_safe=False,
      install_requires=['pyftpdlib', 'python-cloudfiles'],
      scripts=['bin/ftpcloudfs'],
      packages = find_packages(exclude=['tests', 'debian']),
      tests_require = ["nose"],
      classifiers = [
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Programming Language :: Python',
        'Operating System :: OS Independent',
        'Environment :: No Input/Output (Daemon)',
        'License :: OSI Approved :: BSD License',
        ],
      test_suite = "nose.collector",
      )
