#/usr/bin/env python
#
# Author: Chmouel Boudjnah <chmouel.boudjnah@rackspace.co.uk>
#
# Note: Not officially supported by Rackspace but I will be glad to
# help you if you send me an email or leave a comment (preferred
# method) on my blog: http://blog.chmouel.com
import os

from pyftpdlib.filesystems import AbstractedFS
from pyftpdlib.authorizers import DummyAuthorizer, AuthenticationFailed

from fs import CloudFilesFS

class RackspaceCloudFilesFS(CloudFilesFS, AbstractedFS):
    '''Rackspace Cloud Files File system emulation for FTP server.
    '''
    servicenet = False
    authurl = None
    keystone = None

    def __init__(self, username, api_key, servicenet=False, authurl=None, keystone=None):
        CloudFilesFS.__init__(self,
                              username,
                              api_key,
                              servicenet=self.servicenet,
                              authurl=self.authurl,
                              keystone=self.keystone
                              )

    def init_abstracted_fs(self, root, cmd_channel):
        AbstractedFS.__init__(self, root, cmd_channel)

class RackspaceCloudAuthorizer(DummyAuthorizer):
    '''FTP server authorizer. Logs the users into Rackspace Cloud
    Files and keeps track of them.
    '''
    users = {}
    abstracted_fs_for_user = {}

    def validate_authentication(self, username, password, handler):
        '''Validates the username and passwords.  This creates the AbstractedFS at the same time and caches it under the username for retrieval with get_abstracted_fs'''
        try:
            cffs = RackspaceCloudFilesFS(username, password)
        except EnvironmentError, e:
            msg = "Failed to authenticate user %s: %s" % (username, e)
            handler.logerror(msg)
            raise AuthenticationFailed(msg)
        self.abstracted_fs_for_user[username] = cffs
        handler.log("Authentication validated for user %s" % username)

    def get_abstracted_fs(self, username):
        '''Gets an AbstractedFs object for the user.  Raises KeyError
        if username isn't found.'''
        return self.abstracted_fs_for_user.pop(username)

    def has_user(self, username):
        #print '#### has_user', username
        return username != 'anonymous'

    def has_perm(self, username, perm, path=None):
        return True

    def get_perms(self, username):
        return u'lrdw'

    def get_home_dir(self, username):
        return unicode(os.sep)

    def get_msg_login(self, username):
        return u'Welcome %s' % username

    def get_msg_quit(self, username):
        return u'Goodbye %s' % username
