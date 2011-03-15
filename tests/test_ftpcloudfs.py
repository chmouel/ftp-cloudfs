#!/usr/bin/python
import unittest
import os
import sys
import ftplib
import StringIO
from datetime import datetime

from ftpcloudfs.constants import default_address, default_port


class FtpCloudFSTest(unittest.TestCase):
    ''' FTP Cloud FS main test '''

    def setUp(self):
        if not all(['RCLOUD_API_KEY' in os.environ,
                    'RCLOUD_API_USER' in os.environ]):
            print "env RCLOUD_API_USER or RCLOUD_API_KEY not found."
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

    def create_file(self, path, contents):
        '''Create path with contents'''
        self.cnx.storbinary("STOR %s" % path, StringIO.StringIO(contents))

    def test_mkdir_chdir_rmdir(self):
        ''' mkdir/chdir/rmdir directory '''
        directory = "/foobarrandom"
        self.assertEqual(self.cnx.mkd(directory), directory)
        self.assertEqual(self.cnx.cwd(directory),
                         '250 "%s" is the current directory.' % (directory))
        self.assertEqual(self.cnx.rmd(directory), "250 Directory removed.")

    def test_mkdir_chdir_mkdir_rmdir_subdir(self):
        ''' mkdir/chdir/rmdir sub directory '''
        directory = "/foobarrandom"
        self.assertEqual(self.cnx.mkd(directory), directory)
        self.assertEqual(self.cnx.cwd(directory),
                         '250 "%s" is the current directory.' % (directory))
        subdirectory = "potato"
        subdirpath = directory + "/" + subdirectory
        self.assertEqual(self.cnx.mkd(subdirectory), subdirpath)
        # Can't delete a directory with stuff in
        self.assertRaises(ftplib.error_perm, self.cnx.rmd, directory)
        self.assertEqual(self.cnx.cwd(subdirectory),
                         '250 "%s" is the current directory.' % (subdirpath))
        self.assertEqual(self.cnx.cwd(".."),
                         '250 "%s" is the current directory.' % (directory))
        self.assertEqual(self.cnx.rmd(subdirectory), "250 Directory removed.")
        self.assertEqual(self.cnx.cwd(".."),
                         '250 "/" is the current directory.')
        self.assertEqual(self.cnx.rmd(directory), "250 Directory removed.")

    def test_write_open_delete(self):
        ''' write/open/delete file '''
        content_string = "Hello Moto"
        self.create_file("testfile.txt", content_string)
        self.assertEquals(self.cnx.size("testfile.txt"), len(content_string))
        store = StringIO.StringIO()
        self.cnx.retrbinary("RETR testfile.txt", store.write)
        self.assertEqual(store.getvalue(), content_string)
        self.assertEqual(self.cnx.delete("testfile.txt"), "250 File removed.")
        store.close()

    def test_write_open_delete_subdir(self):
        ''' write/open/delete file in a subdirectory'''
        self.cnx.mkd("potato")
        self.cnx.cwd("potato")
        content_string = "Hello Moto"
        self.create_file("testfile.txt", content_string)
        self.assertEquals(self.cnx.size("testfile.txt"), len(content_string))
        store = StringIO.StringIO()
        self.cnx.retrbinary("RETR /ftpcloudfs_testing/potato/testfile.txt", store.write)
        self.assertEqual(store.getvalue(), content_string)
        self.assertEqual(self.cnx.delete("testfile.txt"), "250 File removed.")
        self.cnx.cwd("..")
        self.cnx.rmd("potato")
        store.close()

    def test_write_to_slash(self):
        ''' write to slash should not be permitted '''
        self.cnx.cwd("/")
        content_string = "Hello Moto"
        self.assertRaises(ftplib.error_perm, self.create_file, "testfile.txt", content_string)

    def test_chdir_to_a_file(self):
        ''' chdir to a file '''
        self.create_file("testfile.txt", "Hello Moto")
        self.assertRaises(ftplib.error_perm, self.cnx.cwd, "/ftpcloudfs_testing/testfile.txt")
        self.cnx.delete("testfile.txt")

    def test_chdir_to_slash(self):
        ''' chdir to slash '''
        self.cnx.cwd("/")

    def test_chdir_to_nonexistent_container(self):
        ''' chdir to non existent container'''
        self.assertRaises(ftplib.error_perm, self.cnx.cwd, "/i_dont_exist")

    def test_chdir_to_nonexistent_directory(self):
        ''' chdir to nonexistend directory'''
        self.assertRaises(ftplib.error_perm, self.cnx.cwd, "i_dont_exist")
        self.assertRaises(ftplib.error_perm, self.cnx.cwd, "/ftpcloudfs_testing/i_dont_exist")

    def test_listdir_root(self):
        ''' list root directory '''
        self.cnx.cwd("/")
        ls = self.cnx.nlst()
        self.assertTrue('ftpcloudfs_testing' in ls)
        self.assertTrue('potato' not in ls)
        self.cnx.mkd("potato")
        ls = self.cnx.nlst()
        self.assertTrue('ftpcloudfs_testing' in ls)
        self.assertTrue('potato' in ls)
        self.cnx.rmd("potato")

    def test_listdir(self):
        ''' list directory '''
        content_string = "Hello Moto"
        self.create_file("testfile.txt", content_string)
        self.assertEqual(self.cnx.nlst(), ["testfile.txt"])
        lines = []
        self.assertEquals(self.cnx.retrlines('LIST', callback=lines.append), '226 Transfer complete.')
        self.assertEquals(len(lines), 1)
        line = lines[0]
        self.assertTrue(line.startswith("-rw-r--r--   1 root     root           10 "+ datetime.now().strftime("%b %d %H:")))
        self.assertTrue(line.endswith(" testfile.txt"))
        self.cnx.delete("testfile.txt")

    def test_listdir_subdir(self):
        ''' list a sub directory'''
        content_string = "Hello Moto"
        self.create_file("1.txt", content_string)
        self.create_file("2.txt", content_string)
        self.cnx.mkd("potato")
        self.create_file("potato/3.txt", content_string)
        self.create_file("potato/4.txt", content_string)
        self.assertEqual(self.cnx.nlst(), ["1.txt", "2.txt", "potato"])
        self.cnx.cwd("potato")
        self.assertEqual(self.cnx.nlst(), ["3.txt", "4.txt"])
        self.cnx.delete("3.txt")
        self.cnx.delete("4.txt")
        self.assertEqual(self.cnx.nlst(), [])
        self.cnx.cwd("..")
        self.cnx.delete("1.txt")
        self.cnx.delete("2.txt")
        self.assertEqual(self.cnx.nlst(), ["potato"])
        lines = []
        self.assertEquals(self.cnx.retrlines('LIST', callback=lines.append), '226 Transfer complete.')
        self.assertEquals(len(lines), 1)
        line = lines[0]
        self.assertTrue(line.startswith("drwxr-xr-x   1 root     root            0 "+ datetime.now().strftime("%b %d %H:")))
        self.assertTrue(line.endswith(" potato"))
        self.cnx.rmd("potato")
        self.assertEqual(self.cnx.nlst(), [])

    def test_rename_file(self):
        '''rename a file'''
        content_string = "Hello Moto" * 100
        self.create_file("testfile.txt", content_string)
        self.assertEquals(self.cnx.size("testfile.txt"), len(content_string))
        self.assertRaises(ftplib.error_perm, self.cnx.size, "testfile2.txt")
        self.cnx.rename("testfile.txt", "testfile2.txt")
        self.assertEquals(self.cnx.size("testfile2.txt"), len(content_string))
        self.assertRaises(ftplib.error_perm, self.cnx.size, "testfile.txt")
        self.cnx.delete("testfile2.txt")

    def test_rename_file_into_subdir1(self):
        '''rename a file into a subdirectory 1'''
        content_string = "Hello Moto"
        self.create_file("testfile.txt", content_string)
        self.cnx.mkd("potato")
        self.assertEquals(self.cnx.size("testfile.txt"), len(content_string))
        self.assertRaises(ftplib.error_perm, self.cnx.size, "potato/testfile3.txt")
        self.cnx.rename("testfile.txt", "potato/testfile3.txt")
        self.assertEquals(self.cnx.size("potato/testfile3.txt"), len(content_string))
        self.assertRaises(ftplib.error_perm, self.cnx.size, "testfile.txt")
        self.cnx.delete("potato/testfile3.txt")
        self.cnx.rmd("potato")

    def test_rename_file_into_subdir2(self):
        '''rename a file into a subdirectory without specifying dest leaf'''
        content_string = "Hello Moto"
        self.create_file("testfile.txt", content_string)
        self.cnx.mkd("potato")
        self.assertEquals(self.cnx.size("testfile.txt"), len(content_string))
        self.assertRaises(ftplib.error_perm, self.cnx.size, "potato/testfile.txt")
        self.cnx.rename("testfile.txt", "potato")
        self.assertEquals(self.cnx.size("potato/testfile.txt"), len(content_string))
        self.assertRaises(ftplib.error_perm, self.cnx.size, "testfile.txt")
        self.cnx.delete("potato/testfile.txt")
        self.cnx.rmd("potato")

    def test_rename_file_into_root(self):
        '''rename a file into a subdirectory without specifying dest leaf'''
        content_string = "Hello Moto"
        self.create_file("testfile.txt", content_string)
        self.assertRaises(ftplib.error_perm, self.cnx.rename, "testfile.txt", "/testfile.txt")
        self.cnx.delete("testfile.txt")

    def test_rename_directory_into_file(self):
        '''rename a directory into a file - shouldn't work'''
        content_string = "Hello Moto"
        self.create_file("testfile.txt", content_string)
        self.assertRaises(ftplib.error_perm, self.cnx.rename, "/ftpcloudfs_testing", "testfile.txt")
        self.cnx.delete("testfile.txt")

    def test_rename_directory_into_directory(self):
        '''rename a directory into a directory'''
        self.cnx.mkd("potato")
        self.assertEquals(self.cnx.nlst("potato"), [])
        self.cnx.rename("potato", "potato2")
        self.assertEquals(self.cnx.nlst("potato2"), [])
        self.cnx.rmd("potato2")

    def test_rename_full_directory(self):
        '''rename a directory into a directory'''
        self.cnx.mkd("potato")
        self.create_file("potato/something.txt", "p")
        try:
            self.assertEquals(self.cnx.nlst("potato"), ["something.txt"])
            self.assertRaises(ftplib.error_perm, self.cnx.rename, "potato", "potato2")
        finally:
            self.cnx.delete("potato/something.txt")
            self.cnx.rmd("potato")

    def test_rename_container(self):
        '''rename an empty container'''
        self.cnx.mkd("/potato")
        self.assertEquals(self.cnx.nlst("/potato"), [])
        self.assertRaises(ftplib.error_perm, self.cnx.nlst, "/potato2")
        self.cnx.rename("/potato", "/potato2")
        self.assertRaises(ftplib.error_perm, self.cnx.nlst, "/potato")
        self.assertEquals(self.cnx.nlst("/potato2"), [])
        self.cnx.rmd("/potato2")

    def test_rename_full_container(self):
        '''rename a full container'''
        self.cnx.mkd("/potato")
        self.create_file("/potato/test.txt", "onion")
        self.assertEquals(self.cnx.nlst("/potato"), ["test.txt"])
        self.assertRaises(ftplib.error_perm, self.cnx.rename, "/potato", "/potato2")
        self.cnx.delete("/potato/test.txt")
        self.cnx.rmd("/potato")

    def tearDown(self):
        self.cnx.rmd("/ftpcloudfs_testing")
        self.cnx.close()

if __name__ == '__main__':
    unittest.main()
