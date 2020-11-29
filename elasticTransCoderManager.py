#File: elasticTransCoderManager.py
#Description: This file allows you to create elasticTrancoder Pipelines / Jobs in order to process Video Transcoding jobs
#Author: Farhan Munir
#Created: Nov-29-2020
#Website: https://ronin1770.com
#Patreon: https://www.patreon.com/farhanmunir

import os
import sys
import boto3
import glob

from config import config
from aws_logging import *

from s3BucketManager import *
from iamRoleManager import *

class elasticTransCoderManager(object):
	_logging   = None	

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


	#Create transcoding pipeline	
	# pipeline_name would be used for naming the pipeline
	# input bucket will contain the files to be transcoded
	# output bucket will contain the files after transcoding
	# iam_role_arn is the role that pipeline would use for transcoding
	def create_pipeline(self, pipeline_name, input_bucket, output_bucket, iam_role_arn):
		ret = {}

		client = boto3.client('elastictranscoder')

		try:
			ret = client.create_pipeline(Name=pipeline_name, InputBucket=input_bucket, OutputBucket=output_bucket, Role=iam_role_arn)

		except Exception as e:
			self._logging.create_log( "error", "Exception in create_pipeline:\n{e}")
			return None

		return ret

	# Create the list of presets
	# Input arguments = None
	# Output is a dictionary
	def get_presets(self):
		ret = {}

		client = boto3.client('elastictranscoder')

		try:
			ret = client.list_presets(Ascending="true")

		except Exception as e:
			self._logging.create_log( "error", "Exception in create_pipeline:\n{e}")
			return None

		return ret


	# Create the job for the pipeline
	# Input arguments
	#	pipeline_id - identifies which pipeline to use
	#   input_file_key - represents the file that is needed to be transcoded
	#   outputs - contains the properties for the created file (name of the output file and preset for transcoding properties)
	def create_job(self, pipeline_id, input_file_key, outputs):
		ret = {}

		client = boto3.client('elastictranscoder')

		try:
			ret = client.create_job(PipelineId=pipeline_id,
                                         Input={'Key': input_file_key},
                                         Outputs=outputs)

		except Exception as e:
			self._logging.create_log( "error", f"Exception in create_job:\n{e}")
			return None

		return ret

if __name__ == "__main__":
	etm = elasticTransCoderManager()

	#S3 Bucket manager
	sbm = s3BucketManager()

	#IAM Role Manager
	irm = iamRoleManager()

	#create input bucket
	# Add the extension you want to be uploaded to the bucket
	filter = [".mpg", ".avi", ".mp3"]
	input_bucket_name = "<BUCKET_NAME>"
	output_bucket_name = ""
	sbm.set_input_dir("<SET INPUT DIRECTORY WHERE FILES TO BE UPLOADED ARE KEPT>")
	bucket = sbm.create_s3_bucket(input_bucket_name)

	#Bucket is created successfully, please upload the files to the bucket
	if bucket != None:
		returnval = sbm.upload_files_to_bucket(input_bucket_name, filter)
		etm._logging.create_log( "info", f"Files uploaded: {returnval}")
		

	#Sanity check if the files have been uploaded to the input bucket
	upload_files = sbm.get_files_in_bucket(input_bucket_name)

	if len(upload_files) < 1:
		etm._logging.create_log( "info", "No files have been uploaded. Please first upload files to the input bucket. \nExiting....")
		sys.exit(0)

	#Create the output bucket
	output_bucket = sbm.create_s3_bucket(output_bucket_name)
	if output_bucket == None:
		etm._logging.create_log( "error", "Issue with Output bucket creation. \nExiting....")
		sys.exit(0)

	#In order to create pipeline you will need to first create a role that have permission to do transcoding
	role_name = "<NAME_FOR_TRANCODER_ROLE>"
	policy_name = "<TRANSCODING_POLICY_NAME>"
	role_description = "<TRANSCODING_POLICY_DESCRIPTION>"
	trust_document = { "Version": "2012-10-17", "Statement": [{"Effect": "Allow","Principal": {"Service": "elastictranscoder.amazonaws.com"},"Action": "sts:AssumeRole"}]}

	role_policy = {"Version":"2008-10-17","Statement":[{"Sid":"1","Effect":"Allow","Action":["s3:Put*","s3:ListBucket","s3:*MultipartUpload*","s3:Get*"],"Resource":"*"},{"Sid":"2","Effect":"Allow","Action":"sns:Publish","Resource":"*"},{"Sid":"3","Effect":"Deny","Action":["s3:*Delete*","s3:*Policy*","sns:*Remove*","sns:*Delete*","sns:*Permission*"],"Resource":"*"}]}

	resp = irm.create_iam_role(role_name, role_description, trust_document)
	etm._logging.create_log( "info", f"Response Create Iam Role: \n{resp} \n")

	resp = irm.attach_iam_policy(role_name, policy_name, role_policy)
	etm._logging.create_log( "info",  f"Response Attach Iam Policy: \n{resp} \n")
	
	pipeline_name = "<DESIRED_NAME_PIPELINE>"
	iam_role_arn  = "<ARN_OF_ROLE_CREATED ABOVE>"

	resp = etm.create_pipeline(pipeline_name, input_bucket_name, output_bucket_name, iam_role_arn)

	#Get the Pipeline ID

	if not "Pipeline" in resp:
		etm._logging.create_log( "error", "Error creating pipeline. Exiting. Pipeline data not found")
		exit(0)

	pipeline_id = resp['Pipeline']['Id']

	etm._logging.create_log( "info", f"Pipeline created......{pipeline_id}")

	#Get the files to be converted in the input_bucket
	files_tobe_converted = sbm.get_files_in_bucket(input_bucket)
	files_tobe_converted = files_tobe_converted['Contents']
	

	#Get the list of presets
	presets = etm.get_presets() 
	presets = presets['Presets']

	#Sanity check get the system presets for trancoding
	#Print the System Presets 
	for preset in presets:
		etm._logging.create_log( "info", f"{preset}\n---------------------\n")

	# For sake of this example we have selected following two presets
	generic_1080p_presetid = "1351620000001-000001"
	generic_720p_presetid  = "1351620000001-000010"

	#We need to create Outputs list that contains information about each and file and format for conversion.
	for files in files_tobe_converted:
		outputs = []
		ind = {}
		print( f"{files['Key']} ===== {files['ETag']}" )

		ind = { 'Key' : "generic_1080p_" + files['Key'], "PresetId" : generic_1080p_presetid }
		outputs.append(ind)

		ind = { 'Key' : "generic_720p_" + files['Key'], "PresetId" : generic_720p_presetid }
		outputs.append(ind)

		resp = etm.create_job( pipeline_id, files['Key'], outputs )

		print( f"Processing {files['Key']}:\n\n{resp}")
	