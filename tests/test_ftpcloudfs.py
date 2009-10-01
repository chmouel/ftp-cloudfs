#!/usr/bin/python
import unittest
import os
import sys
import ftplib
import StringIO

from ftpcloudfs.constants import default_address, default_port

class FtpCloudFSTest(unittest.TestCase):
    def setUp(self):
        if not 'RCLOUD_API_KEY' in os.environ or not  'RCLOUD_API_USER' in os.environ:
            print "You need to define the variable RCLOUD_API_USER and RCLOUD_API_KEY"
            sys.exit(1)

        self.username = os.environ['RCLOUD_API_USER']
        self.api_key = os.environ['RCLOUD_API_KEY']
        self.cnx = ftplib.FTP()
        self.cnx.host = default_address
        self.cnx.port = default_port
        self.cnx.connect()
        self.cnx.login(self.username, self.api_key)
        self.cnx.mkd("/ftpcloudfs_testing")
        self.cnx.cwd("/ftpcloudfs_testing")
        
    def test_mkdir_chdir_rmdir(self):
        directory="/foobarrandom"
        self.assertEqual(self.cnx.mkd(directory),directory)
        self.assertEqual(self.cnx.cwd(directory), '250 "%s" is the current directory.' % (directory))
        self.assertEqual(self.cnx.rmd(directory),"250 Directory removed.")

    def test_write_open_delete(self):
        content_string="Hello Moto"
        self.cnx.storbinary("STOR testfile.txt", StringIO.StringIO(content_string))
        store=StringIO.StringIO()
        self.cnx.retrbinary("RETR testfile.txt", store.write )
        self.assertEqual(store.getvalue(), content_string)
        self.assertEqual(self.cnx.delete("testfile.txt"), "250 File removed.")
        store.close()

    def test_chdir_to_a_file(self):
        self.cnx.storbinary("STOR testfile.txt", StringIO.StringIO("Hello Moto"))
        #self.assertRaises does not seems to work no idea why but that works
        try:
            self.cnx.cwd("/ftpcloudfs_testing/testfile.txt")
        except(ftplib.error_perm):
            pass
        else:
            self.assert_(False)
        
        self.cnx.delete("testfile.txt")    

    def test_chdir_to_slash(self):
        self.cnx.cwd("/")
        
    def test_listdir(self):
        content_string="Hello Moto"
        self.cnx.storbinary("STOR testfile.txt", StringIO.StringIO(content_string))
        self.assertEqual(self.cnx.nlst()[0], "testfile.txt")
        self.cnx.delete("testfile.txt")
        
    def tearDown(self):
        self.cnx.rmd("/ftpcloudfs_testing")
        self.cnx.close()

if __name__ == '__main__':
    unittest.main()
    
