#!/Projects/Picker/bin/python3

import re,os,time,datetime,subprocess,sys,base64,random
import os.path, datetime
from shutil import copyfile
import requests
import json
import yaml
import csv
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DateString:

    def __init__(self):
        self.yesterday = str(datetime.date.fromtimestamp(time.time() - (60*60*24) ).strftime("%Y-%m-%d"))
        self.today = str(datetime.date.fromtimestamp(time.time()).strftime("%Y-%m-%d"))
        self.tomorrow = str(datetime.date.fromtimestamp(time.time() + (60*60*24) ).strftime("%Y-%m-%d"))
        self.now = str(time.strftime('%X %x %Z'))


class Files:

    def __init__(self):
        self.dir = ''
        self.data = []
        self.file_exists = 0

    def mkdir(self,dir):
        if not os.path.isdir(dir):
            subprocess.call(["mkdir", dir])
        if os.path.isdir(dir):
            return True
        return False

    def write_csv(self,filename,data):
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            # Write all rows at once
            writer.writerows(data)


    def write_file(self,filename,list):
        f = open(filename,'w')
        for line in list:
            f.write(line + '\n')
        f.close()

    def write_file_append(self,filename,list):
        f = open(filename,'a')
        for line in list:
            f.write(line)
        f.close()

    def write_log(self,logfile,logentry):
        f = open(logfile,'a')
        reportDate =  str(time.strftime("%x - %X"))
        f.write(reportDate + " :" + logentry)
        f.close()

    def read_file(self,filename):
        self.data = []
        with open(filename, 'r') as f:
            lines = f.readlines()
        for line in lines:
            line = line.strip()
            self.data.append(line)
        return self.data

    def copy_file(self,src, dest):
        try:
            copyfile(src, dest)
        except IOError:
            print("Failed file copy ", src,dest)
            sys.exit(2)

    
    def stat_file(self,fname):
        blocksize = 4096
        hash_sha = hashlib.sha256()
        f = open(fname, "rb")
        buf = f.read(blocksize)
        while 1:
            hash_sha.update(buf)
            buf = f.read(blocksize)
            if not buf:
                break    
        checksum =  hash_sha.hexdigest()
        filestat = os.stat(fname)
        filesize = filestat[6]
        return checksum,filesize



