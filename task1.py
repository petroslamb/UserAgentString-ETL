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

def extract_data(filename):
	'''Gets contents of file with key=filename from bucket and returns them as string

	This function has an example of a try, exception. Did not implement
	everywhere because of the time constraint.
	'''
	try:
		k = Key(bucket)
		k.key = filename 
		compressed_string = k.get_contents_as_string()
	except S3ResponseError, err: 
		raise S3Error('Error: %i: %s' % (err.status, err.reason))		
	try:
		return zlib.decompressed(compressed_string)
	except:
		return compressed_string

def currate_data(tsv_string):
	'''TODO: decompress, load tsv, validate? return object or json'''
	print tsv_string 
	return tsv_string 

def transform_data(json_or_string):
	'''TODO: transform return json '''
	print " transformation "
	pass

def load_data(json_string):
	'''Json compress and load to S3'''
	filename ="text.txt.gz" 
	compressed_string = zlib.compress(json_string, 9)
	k = Key(bucket)
	k.key = filename
	k.set_contents_from_string(compressed_string)	
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
			content = extract_data(filename)
			print filename + content +" snatched! \n" 
			new_filename = load_data(content)
			print new_filename + " loaded! \n"
			tsv_content = extract_data(new_filename)
			print new_filename +content +" down! \n"
			currated_content = currate_data(tsv_content)
			transformed_content = transform_data(currated_content)
			new_filename = load_data(transformed_content)	
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
