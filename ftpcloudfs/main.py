# -*- encoding: utf-8 -*-
__author__ = "Chmouel Boudjnah <chmouel@chmouel.com>"

import sys
import os
import signal
import socket
from ConfigParser import RawConfigParser
import logging
from logging.handlers import SysLogHandler

from optparse import OptionParser
import pyftpdlib.servers

from server import ObjectStorageFtpFS
from fs import ObjectStorageFD
from constants import version, default_address, default_port, \
    default_config_file, default_banner, \
    default_ks_tenant_separator, default_ks_service_type, default_ks_endpoint_type
from monkeypatching import MyFTPHandler
from multiprocessing import Manager

def modify_supported_ftp_commands():
    """Remove the FTP commands we don't / can't support, and add the extensions."""
    unsupported = (
        'SITE CHMOD',
    )
    for cmd in unsupported:
        if cmd in pyftpdlib.handlers.proto_cmds:
            del pyftpdlib.handlers.proto_cmds[cmd]
    # add the MD5 command, FTP extension according to IETF Draft:
    # http://tools.ietf.org/html/draft-twine-ftpmd5-00
    pyftpdlib.handlers.proto_cmds.update({
        'MD5': dict(perm=None,
                    auth=True,
                    arg=True,
                    help=u'Syntax: MD5 <SP> file-name (get MD5 of file)')
        })

class Main(object):
    """ftp-cloudfs: A FTP Proxy Interface to OpenStack Object Storage (Swift)."""

    def __init__(self):
        self.options = None

    def setup_log(self):
        """Setup Logging."""

        if self.options.log_level:
            self.options.log_level = logging.DEBUG
        else:
            self.options.log_level = logging.INFO

        if self.options.syslog:
            logger = logging.getLogger()
            try:
                handler = SysLogHandler(address='/dev/log',
                                        facility=SysLogHandler.LOG_DAEMON)
            except IOError:
                # fall back to UDP
                handler = SysLogHandler(facility=SysLogHandler.LOG_DAEMON)
            finally:
                prefix = "%s[%s]: " % (__package__, self.pid)
                formatter = logging.Formatter(prefix + "%(message)s")
                handler.setFormatter(formatter)
                logger.addHandler(handler)
                logger.setLevel(self.options.log_level)
        else:
            log_format = '%(asctime)-15s - %(levelname)s - %(message)s'
            logging.basicConfig(filename=self.options.log_file,
                                format=log_format,
                                level=self.options.log_level)

        # warnings
        if self.config.get("ftpcloudfs", "workers") is not None:
            logging.warning("workers configuration token has been deprecated and has no effect")
        if self.config.get("ftpcloudfs", "service-net") is not None:
            logging.warning("service-net configuration token has been deprecated and has no effect (see ChangeLog)")

    def parse_configuration(self, config_file=default_config_file):
        """Parse the configuration file"""
        config = RawConfigParser({'banner': default_banner,
                                  'port': default_port,
                                  'bind-address': default_address,
                                  'workers': None,
                                  'memcache': None,
                                  'max-cons-per-ip': '0',
                                  'auth-url': None,
                                  'service-net': None,
                                  'verbose': 'no',
                                  'syslog': 'no',
                                  'log-file': None,
                                  'pid-file': None,
                                  'uid': None,
                                  'gid': None,
                                  'masquerade-firewall': None,
                                  'split-large-files': '0',
                                  # keystone auth 2.0 support
                                  'keystone-auth': False,
                                  'keystone-region-name': None,
                                  'keystone-tenant-separator': default_ks_tenant_separator,
                                  'keystone-service-type': default_ks_service_type,
                                  'keystone-endpoint-type': default_ks_endpoint_type,
                                 })
        config.read(default_config_file)
        if not config.has_section('ftpcloudfs'):
            config.add_section('ftpcloudfs')

        self.config = config

    def parse_arguments(self):
        """Parse command line options"""
        parser = OptionParser(version="%prog " + version)
        parser.add_option('-p', '--port',
                          type="int",
                          dest="port",
                          default=self.config.get('ftpcloudfs', 'port'),
                          help="Port to bind the server (default: %d)" % \
                              (default_port))

        parser.add_option('-b', '--bind-address',
                          type="str",
                          dest="bind_address",
                          default=self.config.get('ftpcloudfs', 'bind-address'),
                          help="Address to bind (default: %s)" % \
                              (default_address))

        parser.add_option('-a', '--auth-url',
                          type="str",
                          dest="authurl",
                          default=self.config.get('ftpcloudfs', 'auth-url'),
                          help="Authentication URL (required)")

        memcache = self.config.get('ftpcloudfs', 'memcache')
        if memcache:
            memcache = [x.strip() for x in memcache.split(',')]
        parser.add_option('--memcache',
                          type="str",
                          dest="memcache",
                          action="append",
                          default=memcache,
                          help="Memcache server(s) to be used for cache (ip:port)")

        parser.add_option('-v', '--verbose',
                          action="store_true",
                          dest="log_level",
                          default=self.config.getboolean('ftpcloudfs', 'verbose'),
                          help="Be verbose on logging")

        parser.add_option('-f', '--foreground',
                          action="store_true",
                          dest="foreground",
                          default=False,
                          help="Do not attempt to daemonize but run in foreground")

        parser.add_option('-l', '--log-file',
                          type="str",
                          dest="log_file",
                          default=self.config.get('ftpcloudfs', 'log-file'),
                          help="Log File: Default stdout when in foreground")

        parser.add_option('--syslog',
                          action="store_true",
                          dest="syslog",
                          default=self.config.getboolean('ftpcloudfs', 'syslog'),
                          help="Enable logging to the system logger " + \
                              "(daemon facility)")

        parser.add_option('--pid-file',
                          type="str",
                          dest="pid_file",
                          default=self.config.get('ftpcloudfs', 'pid-file'),
                          help="Pid file location when in daemon mode")

        parser.add_option('--uid',
                          type="int",
                          dest="uid",
                          default=self.config.get('ftpcloudfs', 'uid'),
                          help="UID to drop the privilige to when in daemon mode")

        parser.add_option('--gid',
                          type="int",
                          dest="gid",
                          default=self.config.get('ftpcloudfs', 'gid'),
                          help="GID to drop the privilige to when in daemon mode")

        parser.add_option('--keystone-auth',
                          action="store_true",
                          dest="keystone",
                          default=self.config.get('ftpcloudfs', 'keystone-auth'),
                          help="Use auth 2.0 (Keystone, requires keystoneclient)")

        parser.add_option('--keystone-region-name',
                          type="str",
                          dest="region_name",
                          default=self.config.get('ftpcloudfs', 'keystone-region-name'),
                          help="Region name to be used in auth 2.0")

        parser.add_option('--keystone-tenant-separator',
                          type="str",
                          dest="tenant_separator",
                          default=self.config.get('ftpcloudfs', 'keystone-tenant-separator'),
                          help="Character used to separate tenant_name/username in auth 2.0" + \
                              " (default: TENANT%sUSERNAME)" % default_ks_tenant_separator)

        parser.add_option('--keystone-service-type',
                          type="str",
                          dest="service_type",
                          default=self.config.get('ftpcloudfs', 'keystone-service-type'),
                          help="Service type to be used in auth 2.0 (default: %s)" % default_ks_service_type)

        parser.add_option('--keystone-endpoint-type',
                          type="str",
                          dest="endpoint_type",
                          default=self.config.get('ftpcloudfs', 'keystone-endpoint-type'),
                          help="Endpoint type to be used in auth 2.0 (default: %s)" % default_ks_endpoint_type)

        (options, _) = parser.parse_args()

        if options.keystone:
            try:
                from keystoneclient.v2_0 import client as _test_ksclient
            except ImportError:
                parser.error("Auth 2.0 (keystone) requires python-keystoneclient.")
            keystone_keys = ('region_name', 'tenant_separator', 'service_type', 'endpoint_type')
            options.keystone = dict((key, getattr(options, key)) for key in keystone_keys)

        if not options.authurl:
            parser.error("An authentication URL is required and it wasn't provided")

        self.options = options

    def setup_server(self):
        """Run the main ftp server loop."""
        banner = self.config.get('ftpcloudfs', 'banner').replace('%v', version)
        banner = banner.replace('%f', pyftpdlib.__ver__)

        MyFTPHandler.banner = banner
        ObjectStorageFtpFS.authurl = self.options.authurl
        ObjectStorageFtpFS.keystone = self.options.keystone
        ObjectStorageFtpFS.memcache_hosts = self.options.memcache

        try:
            # store bytes
            ObjectStorageFD.split_size = int(self.config.get('ftpcloudfs', 'split-large-files'))*10**6
        except ValueError, errmsg:
            sys.exit('Split large files error: %s' % errmsg)

        masquerade = self.config.get('ftpcloudfs', 'masquerade-firewall')
        if masquerade:
            try:
                MyFTPHandler.masquerade_address = socket.gethostbyname(masquerade)
            except socket.gaierror, (_, errmsg):
                sys.exit('Masquerade address error: %s' % errmsg)

        try:
            max_cons_per_ip = int(self.config.get('ftpcloudfs', 'max-cons-per-ip'))
        except ValueError, errmsg:
            sys.exit('Max connections per IP error: %s' % errmsg)

        ftpd = pyftpdlib.servers.MultiprocessFTPServer((self.options.bind_address,
                                                        self.options.port),
                                                       MyFTPHandler,
                                                       )

        # set it to unlimited, we use our own checks with a shared dict
        ftpd.max_cons_per_ip = 0
        ftpd.handler.max_cons_per_ip = max_cons_per_ip

        return ftpd

    def setup_daemon(self, preserve=None):
        """Setup the daemon context for the server."""
        import daemon
        from utils import PidFile
        import tempfile

        daemonContext = daemon.DaemonContext()

        if not self.options.pid_file:
            self.options.pid_file = "%s/ftpcloudfs.pid" % \
                (tempfile.gettempdir())

        self.pidfile = PidFile(self.options.pid_file)
        daemonContext.pidfile = self.pidfile
        if self.options.uid:
            daemonContext.uid = self.options.uid

        if self.options.gid:
            daemonContext.gid = self.options.gid

        if preserve:
            daemonContext.files_preserve = preserve

        return daemonContext

    def signal_handler(self, signal, frame):
        """Catch signals and propagate them to child processes."""
        if self.shm_manager:
            self.shm_manager.shutdown()
            self.shm_manager = None
        self.old_signal_handler(signal, frame)

    def main(self):
        """Main entry point."""
        self.pid = os.getpid()
        self.parse_configuration()
        self.parse_arguments()
        modify_supported_ftp_commands()

        ftpd = self.setup_server()

        if self.options.foreground:
            MyFTPHandler.shared_ip_map = None
            self.setup_log()
            ftpd.serve_forever()
            return

        daemonContext = self.setup_daemon([ftpd.socket.fileno(), ftpd.ioloop.fileno(),])
        with daemonContext:
            self.old_signal_handler = signal.signal(signal.SIGTERM, self.signal_handler)

            self.shm_manager = Manager()
            MyFTPHandler.shared_ip_map = self.shm_manager.dict()
            MyFTPHandler.shared_lock = self.shm_manager.Lock()

            self.setup_log()
            ftpd.serve_forever()

