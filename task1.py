#!/usr/bin/python
import time
import os
import boto
import uuid
from boto.s3.connection import S3Connection
from datetime import datetime


def update_list(lista):
	updated_filelist = []
	for key in bucket.list():
		k =key.name.encode('utf-8')
		if k not in lista:
			updated_filelist.append(k)
	return updated_filelist

def daemon():

	filepath = './sample.txt'
	dirpath = os.path.dirname(filepath)

	while True:
		if not os.path.exists(dirpath) or not os.path.isdir(dirpath):
			os.makedirs(dirpath)

		f = open(filepath,'w')
		f.write(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
		f.close()
		time.sleep(2)

if __name__=="__main__":


	conn =S3Connection()
	bucket = conn.get_bucket('kuvas')
	filelist = []
	filelist = update_list(filelist)
	print filelist
	daemon()
