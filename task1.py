#!/usr/bin/python

'''

This is the daemon service that handles download, de/compression, and file upload.

Usage: run the file which includes daemon process

Daemon could be implemented with python-daemon for niceness.

Although the test explicitly states that there should be manipulation through files, 
send_file returned a BadRequest so I implemented everything with contents anyway.
Its cleaner and could have speed advantages, although network is the bottleneck here.

TODO: After implementing service, exception handling could log certain errors, 
without halting daemon.

'''

 
import time, re
import os, zlib 
import boto 
import uuid
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError
import csv
from user_agents import parse
import json 
import transform

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
		if key.key not in lista:
			if key.key.startswith(u'infrastructure/logs'):
				updated_filelist.append(key.key)
	return updated_filelist

def filename_to_push(filename):
	'''Creates filename to push back to S3'''
	name = re.sub(r'\W+', ' ', filename)
	name_as_words = name.split()
	name_as_words = name_as_words[2:-1]
	prepended_words = [u'infrastructure',u'processed',u'teng_petros_lambropoulos']
	push_name_in_words = prepended_words + name_as_words
	push_name = '/'.join(push_name_in_words) + u'.gz'
	return push_name 

def extract_data(filename):
	'''Gets contents of file with key=filename from bucket and returns them as string

	This function has an example of a try, exception. Did not implement
	everywhere because of time constraints.
	'''
	try:
		k = Key(bucket)
		k.key = filename
		push_filename = filename_to_push(filename) 
		compressed_string = k.get_contents_as_string()
	except S3ResponseError, err: 
		raise S3Error('Error: %i: %s' % (err.status, err.reason))		
	try:
		return push_filename, zlib.decompress(compressed_string, zlib.MAX_WBITS|32)
	except DecompressException:
		print 'Could not decompress file ' + filename

def currate_data(tsv_string):
	''' Clean data if needed. 

	This is a good place to do validation.
	'''
	reader = csv.reader(tsv_string.split('\n'), delimiter = '\t', quoting=csv.QUOTE_NONE)
	return reader 

def transform_data(tsv_string):
	'''Transform, return json.

	This was made into a separate module (transport) for clarity.
	'''
	return transform.make_all_json_responses(tsv_string)
	

def load_data(push_name, json_string):
	'''TODO: Json compress and load to S3'''
	compressed_string = zlib.compress(json_string, 9)
	k = Key(bucket)
	k.key = push_name 
	k.set_contents_from_string(compressed_string)	
	return compressed_string 
	

def daemon():
	'''Main control function, the daemon runs every few seconds'''
	filelist = []

	while True:
	
		updated_filelist = [] 
		updated_filelist = list_update(filelist)
		
		for filename in updated_filelist:

			push_filename, tsv_content = extract_data(filename)
			print filename +  " snatched! \n" 

			currated_content = currate_data(tsv_content)
			print filename + " currated! \n" 

			transformed_content = transform_data(currated_content)
			print push_filename + " transformed! \n" 

			#compressed_string = load_data(push_filename, transformed_content)
			#print filename + " pushed! \n" 
	
		# Filelist should be saved to file and reloaded to help with service errors
		# A cleanup script should also clean that file when we need to rerun from scratch.
		filelist.extend(updated_filelist)	
		time.sleep(2)

if __name__=="__main__":


	conn = S3Connection(aws_access_key_id=env_key_id, aws_secret_access_key=env_access_key, profile_name=env_profile)
	bucket = conn.get_bucket('af-testcs')
	daemon()
