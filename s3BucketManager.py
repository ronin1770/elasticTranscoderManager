#File: s3BucketManager.py
#Description: This file allows you to manage S3 Buckets. Creating / getting bucket contents
#Author: Farhan Munir
#Created: Nov-21-2020
#Website: https://ronin1770.com

import os
import sys
import boto3
import glob

from config import config
from aws_logging import *

class s3BucketManager(object):

	_logging   = None
	_input_dir = ""   #Directory on the local system where files are kept for uploading

	#setting / getter for input_dir
	def set_input_dir(self, input_dir):
		self._input_dir = input_dir

	def get_input_dir(self):
		return self._input_dir

	#Constructor
	def __init__(self):
		self._logging = aws_logging()


		# Create the resource for EC2 creator
		if self.check_aws_configuration_exists() == False:
			self._logging.create_log( "error", "AWS Credentials file not found")
			sys.exit(0)
		else:
			self._logging.create_log( "info", "AWS Credentials file found successfully")

	#Check if the AWS configuration file exists - it not throw an error
	# It should exist in ~/.aws/credentials 
	def check_aws_configuration_exists(self):
		return os.path.isfile(config['aws_creds_location'])

	#This function creates a bucket  specified by the bucket name in the default AWS region	
	#Input parameters - bucketname
	#Bucket will be created in your default region	
	#Incase of success directory object will be shared
	#Incase of an error None is return
	def create_s3_bucket(self, bucketname):
		try:
			s3_client = boto3.client('s3')
			return s3_client.create_bucket(Bucket=bucketname)

		except Exception as e:
			self._logging.create_log("error", f"Exception in create_s3_bucket:\n {e}")
			return None
		return None

	#This function uploads / Pushes file(s) from input directory to the bucket based on the filter 
	#Input argument filter containing the extensions 
	def upload_files_to_bucket(self, bucket, filter):
		ret = []
		s3_client = boto3.client('s3')

		try:
			all_files = glob.glob(f"{self._input_dir}/*.*")

			for file in all_files:
				filename, file_extension = os.path.splitext(file)
				if str(file_extension) in filter:
					print(f"Uploading {filename}")

					with open(file, 'rb') as data:
						response = s3_client.upload_fileobj(data, bucket, os.path.basename(filename) + file_extension )
						ret.append(response)

		except Exception as e:
			self._logging.create_log("error", f"Exception in upload_files_to_bucket:\n {e}")
			return None
		return ret

	#This function retrieves the list of files in the bucket (identified by the bucket name)
	#input - bucket name - String
	#Output - list of objects
	def get_files_in_bucket(self, bucketname):
		ret = []

		s3_client = boto3.client("s3")

		try:
			ret = s3_client.list_objects(Bucket=bucketname)

		except Exception as e:
			self._logging.create_log("error", f"Exception in get_files_in_bucket:\n {e}")
			return None

		return ret