#!/usr/bin/python
import unittest
import os
import sys
import ftplib
import StringIO
from datetime import datetime
import cloudfiles
from ftpcloudfs.fs import CloudFilesFS

import logging
#logging.getLogger().setLevel(logging.DEBUG)
#logging.basicConfig(level=logging.DEBUG)

class CloudFilesFSTest(unittest.TestCase):
    '''CloudFilesFS Test'''

    def setUp(self):
        if not hasattr(self, 'username'):
            cls = self.__class__
            if not all(['RCLOUD_API_KEY' in os.environ,
                        'RCLOUD_API_USER' in os.environ]):
                print "env RCLOUD_API_USER or RCLOUD_API_KEY not found."
                sys.exit(1)
            cls.username = os.environ['RCLOUD_API_USER']
            cls.api_key = os.environ['RCLOUD_API_KEY']
            cls.auth_url = os.environ.get('RCLOUD_AUTH_URL')
            cls.cnx = CloudFilesFS(self.username, self.api_key, authurl=self.auth_url)
            cls.conn = cloudfiles.get_connection(self.username, self.api_key, authurl=self.auth_url)
        self.cnx.mkdir("/ftpcloudfs_testing")
        self.cnx.chdir("/ftpcloudfs_testing")
        self.container = self.conn.get_container('ftpcloudfs_testing')

    def create_file(self, path, contents):
        '''Create path with contents'''
        fd = self.cnx.open(path, "wb")
        fd.write(contents)
        fd.close()

    def read_file(self, path):
        fd = self.cnx.open(path, "rb")
        contents = fd.read()
        fd.close()
        return contents

    def test_mkdir_chdir_rmdir(self):
        ''' mkdir/chdir/rmdir directory '''
        directory = "/foobarrandom"
        self.cnx.mkdir(directory)
        self.cnx.chdir(directory)
        self.assertEqual(self.cnx.getcwd(), directory)
        self.assertEqual(self.cnx.listdir(directory), [])
        self.cnx.rmdir(directory)

    def test_mkdir_chdir_mkdir_rmdir_subdir(self):
        ''' mkdir/chdir/rmdir sub directory '''
        directory = "/foobarrandom"
        self.cnx.mkdir(directory)
        self.cnx.chdir(directory)
        subdirectory = "potato"
        subdirpath = directory + "/" + subdirectory
        self.cnx.mkdir(subdirectory)
        # Can't delete a directory with stuff in
        self.assertRaises(EnvironmentError, self.cnx.rmdir, directory)
        self.cnx.chdir(subdirectory)
        self.cnx.chdir("..")
        self.assertEqual(self.cnx.getcwd(), directory)
        self.cnx.rmdir(subdirectory)
        self.cnx.chdir("..")
        self.cnx.rmdir(directory)

    def test_write_open_delete(self):
        ''' write/open/delete file '''
        content_string = "Hello Moto"
        self.create_file("testfile.txt", content_string)
        self.assertEquals(self.cnx.getsize("testfile.txt"), len(content_string))
        contents = self.read_file("testfile.txt")
        self.assertEqual(contents, content_string)
        self.cnx.remove("testfile.txt")

    def test_write_open_delete_subdir(self):
        ''' write/open/delete file in a subdirectory'''
        self.cnx.mkdir("potato")
        self.cnx.chdir("potato")
        content_string = "Hello Moto"
        self.create_file("testfile.txt", content_string)
        self.assertEquals(self.cnx.getsize("testfile.txt"), len(content_string))
        content = self.read_file("/ftpcloudfs_testing/potato/testfile.txt")
        self.assertEqual(content, content_string)
        self.cnx.remove("testfile.txt")
        self.cnx.chdir("..")
        self.cnx.rmdir("potato")

    def test_write_to_slash(self):
        ''' write to slash should not be permitted '''
        self.cnx.chdir("/")
        content_string = "Hello Moto"
        self.assertRaises(EnvironmentError, self.create_file, "testfile.txt", content_string)

    def test_chdir_to_a_file(self):
        ''' chdir to a file '''
        self.create_file("testfile.txt", "Hello Moto")
        self.assertRaises(EnvironmentError, self.cnx.chdir, "/ftpcloudfs_testing/testfile.txt")
        self.cnx.remove("testfile.txt")

    def test_chdir_to_slash(self):
        ''' chdir to slash '''
        self.cnx.chdir("/")

    def test_chdir_to_nonexistent_container(self):
        ''' chdir to non existent container'''
        self.assertRaises(EnvironmentError, self.cnx.chdir, "/i_dont_exist")

    def test_chdir_to_nonexistent_directory(self):
        ''' chdir to nonexistend directory'''
        self.assertRaises(EnvironmentError, self.cnx.chdir, "i_dont_exist")
        self.assertRaises(EnvironmentError, self.cnx.chdir, "/ftpcloudfs_testing/i_dont_exist")

    def test_listdir_root(self):
        ''' list root directory '''
        self.cnx.chdir("/")
        dt = abs(datetime.utcfromtimestamp(self.cnx.getmtime("/")) - datetime.utcnow())
        self.assertTrue(dt.seconds < 60)
        ls = self.cnx.listdir(".")
        self.assertTrue('ftpcloudfs_testing' in ls)
        dt = abs(datetime.utcfromtimestamp(self.cnx.getmtime("ftpcloudfs_testing")) - datetime.utcnow())
        self.assertTrue(dt.seconds < 60)
        self.assertTrue('potato' not in ls)
        self.cnx.mkdir("potato")
        ls = self.cnx.listdir(".")
        self.assertTrue('ftpcloudfs_testing' in ls)
        self.assertTrue('potato' in ls)
        self.cnx.rmdir("potato")

    def test_listdir(self):
        ''' list directory '''
        content_string = "Hello Moto"
        self.create_file("testfile.txt", content_string)
        dt = abs(datetime.utcfromtimestamp(self.cnx.getmtime("testfile.txt")) - datetime.utcnow())
        self.assertTrue(dt.seconds < 60)
        self.assertEqual(self.cnx.listdir("."), ["testfile.txt"])
        self.cnx.remove("testfile.txt")

    def test_listdir_subdir(self):
        ''' list a sub directory'''
        content_string = "Hello Moto"
        self.create_file("1.txt", content_string)
        self.create_file("2.txt", content_string)
        self.cnx.mkdir("potato")
        self.create_file("potato/3.txt", content_string)
        self.create_file("potato/4.txt", content_string)
        self.assertEqual(self.cnx.listdir("."), ["1.txt", "2.txt", "potato"])
        self.cnx.chdir("potato")
        self.assertEqual(self.cnx.listdir("."), ["3.txt", "4.txt"])
        self.cnx.remove("3.txt")
        self.cnx.remove("4.txt")
        self.assertEqual(self.cnx.listdir("."), [])
        self.cnx.chdir("..")
        self.cnx.remove("1.txt")
        self.cnx.remove("2.txt")
        self.assertEqual(self.cnx.listdir("."), ["potato"])
        dt = abs(datetime.utcfromtimestamp(self.cnx.getmtime("potato")) - datetime.utcnow())
        self.assertTrue(dt.seconds < 60)
        self.cnx.rmdir("potato")
        self.assertEqual(self.cnx.listdir("."), [])

    def test_rename_file(self):
        '''rename a file'''
        content_string = "Hello Moto" * 100
        self.create_file("testfile.txt", content_string)
        self.assertEquals(self.cnx.getsize("testfile.txt"), len(content_string))
        self.assertRaises(EnvironmentError, self.cnx.getsize, "testfile2.txt")
        self.cnx.rename("testfile.txt", "testfile2.txt")
        self.assertEquals(self.cnx.getsize("testfile2.txt"), len(content_string))
        self.assertRaises(EnvironmentError, self.cnx.getsize, "testfile.txt")
        self.cnx.remove("testfile2.txt")

    def test_rename_file_into_subdir1(self):
        '''rename a file into a subdirectory 1'''
        content_string = "Hello Moto"
        self.create_file("testfile.txt", content_string)
        self.cnx.mkdir("potato")
        self.assertEquals(self.cnx.getsize("testfile.txt"), len(content_string))
        self.assertRaises(EnvironmentError, self.cnx.getsize, "potato/testfile3.txt")
        self.cnx.rename("testfile.txt", "potato/testfile3.txt")
        self.assertEquals(self.cnx.getsize("potato/testfile3.txt"), len(content_string))
        self.assertRaises(EnvironmentError, self.cnx.getsize, "testfile.txt")
        self.cnx.remove("potato/testfile3.txt")
        self.cnx.rmdir("potato")

    def test_rename_file_into_subdir2(self):
        '''rename a file into a subdirectory without specifying dest leaf'''
        content_string = "Hello Moto"
        self.create_file("testfile.txt", content_string)
        self.cnx.mkdir("potato")
        self.assertEquals(self.cnx.getsize("testfile.txt"), len(content_string))
        self.assertRaises(EnvironmentError, self.cnx.getsize, "potato/testfile.txt")
        self.cnx.rename("testfile.txt", "potato")
        self.assertEquals(self.cnx.getsize("potato/testfile.txt"), len(content_string))
        self.assertRaises(EnvironmentError, self.cnx.getsize, "testfile.txt")
        self.cnx.remove("potato/testfile.txt")
        self.cnx.rmdir("potato")

    def test_rename_file_into_root(self):
        '''rename a file into a subdirectory without specifying dest leaf'''
        content_string = "Hello Moto"
        self.create_file("testfile.txt", content_string)
        self.assertRaises(EnvironmentError, self.cnx.rename, "testfile.txt", "/testfile.txt")
        self.cnx.remove("testfile.txt")

    def test_rename_directory_into_file(self):
        '''rename a directory into a file - shouldn't work'''
        content_string = "Hello Moto"
        self.create_file("testfile.txt", content_string)
        self.assertRaises(EnvironmentError, self.cnx.rename, "/ftpcloudfs_testing", "testfile.txt")
        self.cnx.remove("testfile.txt")

    def test_rename_directory_into_directory(self):
        '''rename a directory into a directory'''
        self.cnx.mkdir("potato")
        self.assertEquals(self.cnx.listdir("potato"), [])
        self.cnx.rename("potato", "potato2")
        self.assertEquals(self.cnx.listdir("potato2"), [])
        self.cnx.rmdir("potato2")

    def test_rename_directory_into_existing_directory(self):
        '''rename a directory into an existing directory'''
        self.cnx.mkdir("potato")
        self.cnx.mkdir("potato2")
        self.assertEquals(self.cnx.listdir("potato"), [])
        self.assertEquals(self.cnx.listdir("potato2"), [])
        self.cnx.rename("potato", "potato2")
        self.assertEquals(self.cnx.listdir("potato2"), ["potato"])
        self.assertEquals(self.cnx.listdir("potato2/potato"), [])
        self.cnx.rmdir("potato2/potato")
        self.cnx.rmdir("potato2")

    def test_rename_directory_into_self(self):
        '''rename a directory into itself'''
        self.cnx.mkdir("potato")
        self.assertEquals(self.cnx.listdir("potato"), [])
        self.cnx.rename("potato", "/ftpcloudfs_testing")
        self.assertEquals(self.cnx.listdir("potato"), [])
        self.cnx.rename("potato", "/ftpcloudfs_testing/potato")
        self.assertEquals(self.cnx.listdir("potato"), [])
        self.cnx.rename("potato", "potato")
        self.assertEquals(self.cnx.listdir("potato"), [])
        self.cnx.rename("/ftpcloudfs_testing/potato", ".")
        self.assertEquals(self.cnx.listdir("potato"), [])
        self.cnx.rmdir("potato")

    def test_rename_full_directory(self):
        '''rename a directory into a directory'''
        self.cnx.mkdir("potato")
        self.create_file("potato/something.txt", "p")
        try:
            self.assertEquals(self.cnx.listdir("potato"), ["something.txt"])
            self.assertRaises(EnvironmentError, self.cnx.rename, "potato", "potato2")
        finally:
            self.cnx.remove("potato/something.txt")
            self.cnx.rmdir("potato")

    def test_rename_container(self):
        '''rename an empty container'''
        self.cnx.mkdir("/potato")
        self.assertEquals(self.cnx.listdir("/potato"), [])
        self.assertRaises(EnvironmentError, self.cnx.listdir, "/potato2")
        self.cnx.rename("/potato", "/potato2")
        self.assertRaises(EnvironmentError, self.cnx.listdir, "/potato")
        self.assertEquals(self.cnx.listdir("/potato2"), [])
        self.cnx.rmdir("/potato2")

    def test_rename_full_container(self):
        '''rename a full container'''
        self.cnx.mkdir("/potato")
        self.create_file("/potato/test.txt", "onion")
        self.assertEquals(self.cnx.listdir("/potato"), ["test.txt"])
        self.assertRaises(EnvironmentError, self.cnx.rename, "/potato", "/potato2")
        self.cnx.remove("/potato/test.txt")
        self.cnx.rmdir("/potato")

    def test_unicode_file(self):
        '''Test unicode file creation'''
        # File names use a utf-8 interface
        file_name = u"Smiley\u263a.txt".encode("utf-8")
        self.create_file(file_name, "Hello Moto")
        self.assertEqual(self.cnx.listdir("."), [file_name])
        self.cnx.remove(file_name)

    def test_unicode_directory(self):
        '''Test unicode directory creation'''
        # File names use a utf-8 interface
        dir_name = u"Smiley\u263aDir".encode("utf-8")
        self.cnx.mkdir(dir_name)
        self.assertEqual(self.cnx.listdir("."), [dir_name])
        self.cnx.rmdir(dir_name)

    def test_mkdir_container_unicode(self):
        ''' mkdir/chdir/rmdir directory '''
        directory = u"/Smiley\u263aContainer".encode("utf-8")
        self.cnx.mkdir(directory)
        self.cnx.chdir(directory)
        self.cnx.rmdir(directory)

    def test_fakedir(self):
        '''Make some fake directories and test'''

        obj1 = self.container.create_object("test1.txt")
        obj1.content_type = "text/plain"
        obj1.write("Hello Moto")

        obj2 = self.container.create_object("potato/test2.txt")
        obj2.content_type = "text/plain"
        obj2.write("Hello Moto")

        obj3 = self.container.create_object("potato/sausage/test3.txt")
        obj3.content_type = "text/plain"
        obj3.write("Hello Moto")

        obj4 = self.container.create_object("potato/sausage/test4.txt")
        obj4.content_type = "text/plain"
        obj4.write("Hello Moto")

        self.assertEqual(self.cnx.listdir("."), ["potato", "test1.txt"])
        self.assertEqual(self.cnx.listdir("potato"), ["sausage","test2.txt"])
        self.assertEqual(self.cnx.listdir("potato/sausage"), ["test3.txt", "test4.txt"])

        self.cnx.chdir("potato")

        self.assertEqual(self.cnx.listdir("."), ["sausage","test2.txt"])
        self.assertEqual(self.cnx.listdir("sausage"), ["test3.txt", "test4.txt"])

        self.cnx.chdir("sausage")

        self.assertEqual(self.cnx.listdir("."), ["test3.txt", "test4.txt"])

        self.cnx.chdir("../..")

        self.container.delete_object(obj1.name)
        self.container.delete_object(obj2.name)
        self.container.delete_object(obj3.name)
        self.container.delete_object(obj4.name)

        self.assertEqual(self.cnx.listdir("."), [])

    def tearDown(self):
        # Delete eveything from the container using the API
        fails = self.container.list_objects()
        for obj in fails:
            self.container.delete_object(obj)
        self.cnx.rmdir("/ftpcloudfs_testing")
        self.assertEquals(fails, [], "The test failed to clean up after itself leaving these objects: %r" % fails)

if __name__ == '__main__':
    unittest.main()
