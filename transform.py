#!/usr/bin/python

#import csv
#from user_agents import parse
#import redis
#import re
#import json 
#from urllib2 import urlopen
import sys
sys.path.append('/home/pet/affectv/geodis/src/')
import src.geodis as geodis

conn = geodis.redis.Redis(db=8)

def make_all_json_responses(tsv_string):
	'''Main function of this module

	This function loops through the strings and returns the json objects as a list
	'''
	json_responses = []
	reader = csv.reader(tsv_string.split('\n'), delimiter = '\t', quoting=csv.QUOTE_NONE)
	for row_list in reader:
		if row_list: 
			ua_string = row_list[4]
			ua_agent = parse(ua_string)
			json_response = make_json_response(row_list, ua_agent)
			json_responses.append(json_response)

	return '\n'.join(json_responses)


def make_json_response(row_list, ua_agent):
	'''Creates the JSON object that will be pushed back to S3'''
	geo_string = row_list[2]
	la, lo = re.match("\[(.*?)\,(.*?)\]",geo_string).groups()
	url = row_list[3]
	city, country = getplace(la,lo)
	mobile =ua_agent.is_mobile
	os_family = ua_agent.os
	browser_family = ua_agent.browser.family
	os_string = ua_agent.ua_string
	
	#This might look better if declared globally and variables were passed with .replace()
	json_string =[
	
				{
				'location': {
							'city': city,
							'country': country, 
							'latitude': la,
							'longitude': lo
							},
				'timestamp': row_list[0], 
				'url': url, 
				'user_agent': {
							'browser_family': browser_family, 
							'mobile': mobile, 
							'os_family': os_family, 
							'string': os_string
							}, 
				'user_id': row_list[1]
				}
	
				]
	
	json_obj = json.dumps(json_string)
	return json_obj
	
	
def getplace(lat,lon):
	'''Uses the geodis module (doat/geodis in Github) to resolve city, country'''
	geo_obj = geodis.City.getByLatLon(float(lat), float(lon), conn)			
	city = geo_obj.name
	country = geo_obj.country
	return city, country 
