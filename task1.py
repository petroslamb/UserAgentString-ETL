#!/usr/bin/python

'''

Usage: run the file which includes daemon process

Daemon could be implemented with python-daemon for niceness.

Although the test explicitly states that there should be manipulation through files, 
send_file returned a BadRequest so I implemented everything with contents anyway.
Its cleaner and could have speed advantages, although network the is the bottleneck here.

'''

 
import time
import os, sys, gzip, zlib
import boto as boto
import uuid
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError
from datetime import datetime

class S3Error(Exception):
	"Misc. S3 Service Error"
	pass

def list_update(lista):
	'''Checks if bucket has new files and returns a list of them'''
	updated_filelist = []
	for key in bucket.list():
		k =key.name.encode('utf-8')
		if k not in lista:
			updated_filelist.append(k)
	return updated_filelist

def snatch_file_content(filename):
	'''Gets contents of file with key=filename from bucket and returns them as string

	This function has an example of a try, exception. Did not implement
	everywhere because of the time constraint.
	'''
	try:
		k = Key(bucket)
		k.key = filename 
		return k.get_contents_as_string()
	except S3ResponseError, err: 
		raise S3Error('Error: %i: %s' % (err.status, err.reason))		

def snatch_file(filename):
	'''Gets file with key=filename and returns open file handle'''
	print "snatched file"
	return filename


def extract_data(filegz):
	'''TODO: decompress, load tsv, validate? return object or json'''
	print filegz
	return filegz

def transform_data(json_or_string):
	'''TODO: transform return json '''
	print " transformation "
	pass

def load_data(json_string):
	'''Create file compress and load to S3'''
	print json_string
	filename ="text.txt.gz" 
	compressed_string = zlib.compress(json_string, 9)
	k = Key(bucket)
	k.key = filename
	k.set_contents_from_string(compressed_string)	

	#Code with file manipulation
	#gz_file = gzip.open(filename, 'wb')
	#gz_file.write(json_string)
	#gz_file.close()
	#k = Key(bucket)
	#k.key = filename
	#k.set_contents_from_filename(filename)
	return filename
	
		

def daemon():
	'''Main control function, the daemon runs every few seconds'''
	filelist = []
	#filepath = './sample.txt'
	#dirpath = os.path.dirname(filepath)

	while True:
	
		updated_filelist = [] 
		updated_filelist = list_update(filelist)
		
		for filename in updated_filelist:
			content = snatch_file_content(filename)
			print filename + " snatched! \n" 
			new_filename = load_data(content)

			filegz = snatch_file(new_filename)
			extracted_content = extract_data(filegz)
			transformed_content = transform_data(extracted_content)
			
		#if not os.path.exists(dirpath) or not os.path.isdir(dirpath):
		#	os.makedirs(dirpath)
		#f = open(filepath,'w')
		#f.write(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
		#f.close()
		filelist.extend(updated_filelist)	
		time.sleep(2)

if __name__=="__main__":


	conn =S3Connection()
	bucket = conn.get_bucket('kuvas')
	daemon()
