#!/usr/bin/python

'''

Usage: run the file which includes daemon process

Daemon could be implemented with python-daemon for niceness.

Although the test explicitly states that there should be manipulation through files, 
send_file returned a BadRequest so I implemented everything with contents anyway.
Its cleaner and could have speed advantages, although network is the bottleneck here.

'''

 
import time
import os, sys, gzip, zlib, tempfile
import boto as boto
import uuid
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError
from datetime import datetime

class S3Error(Exception):
	"Misc. S3 Service Error"
	pass

env_profile = os.environ.get('AWS_USERNAME')
env_key_id = os.environ['AWS_KEY_ID']
env_access_key = os.environ['AWS_ACCESS_KEY']

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
	everywhere because of time constraints.
	'''
	try:
		k = Key(bucket)
		k.key = filename 
		compressed_string = k.get_contents_as_string()
	except S3ResponseError, err: 
		raise S3Error('Error: %i: %s' % (err.status, err.reason))		
	try:
		
		return zlib.decompress(compressed_string, zlib.MAX_WBITS|32)
	except:
		return compressed_string

def currate_data(tsv_string):
	''' Clean data if needed '''
	return tsv_string 

def transform_data(json_or_string):
	'''TODO: transform return json '''
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

	while True:
	
		updated_filelist = [] 
		updated_filelist = list_update(filelist)
		
		for filename in updated_filelist:
			tsv_content = extract_data(filename)
			print filename + tsv_content + " snatched! \n" 
			currated_content = currate_data(tsv_content)
			print filename + " currated! \n" 
			transformed_content = transform_data(currated_content)
			print filename + " transformed! \n" 
			#new_filename = load_data(transformed_content)
			#print filename + " pushed! \n" 
	
		# Filelist should be saved to file and reloaded to help with service errors
		# A cleanup file should also be mustered.
		filelist.extend(updated_filelist)	
		time.sleep(2)

if __name__=="__main__":


	conn = S3Connection(aws_access_key_id=env_key_id, aws_secret_access_key=env_access_key, profile_name=env_profile)
	bucket = conn.get_bucket('af-testcs')
	daemon()
