#!/usr/bin/python
import unittest
import os
import sys
import ftplib
import StringIO

SERVER_IP="127.0.0.1"
SERVER_PORT=2021

class FtpCloudFSTest(unittest.TestCase):
    def setUp(self):
        if not 'RCLOUD_API_KEY' in os.environ or not  'RCLOUD_API_USER' in os.environ:
            print "You need to define the variable RCLOUD_API_USER and RCLOUD_API_KEY"
            sys.exit(1)

        self.username = os.environ['RCLOUD_API_USER']
        self.api_key = os.environ['RCLOUD_API_KEY']
        self.cnx = ftplib.FTP()
        self.cnx.host = SERVER_IP
        self.cnx.port = SERVER_PORT
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
    
