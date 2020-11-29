#File: IamRoleManager.py
#Description: This file allows you to create I AM Role for AWS
#Author: Farhan Munir
#Created: Nov-24-2020
#Website: https://ronin1770.com

import os
import sys
import boto3
import json

from config import config
from aws_logging import *


class iamRoleManager(object):
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

	#This function creates an IAM identified by role_name 
	#Trust Policy Document for the role is defined in the json_str
	#In case of success dictionary object is returned
	#In case of failure None is returned
	def create_iam_role(self, role_name, role_description, json_str):
		ret = {}

		client = boto3.client("iam")

		try:
			assume_role_policy_document = json.dumps(json_str)
			print (assume_role_policy_document)
			ret =  client.create_role(AssumeRolePolicyDocument=assume_role_policy_document, RoleName=role_name, Description=role_description)

		except Exception as e:
			self._logging.create_log( "error", f"Exception in create_iam_role:\n{e}")
			return None
		return ret

	#This function attaches the role policy to the specified role
	#Role is identified by the role_name
	#Policy_name provides the policy a unique name
	#Policy_document identifies the policy for the role
	#In case of success dictionary object is returned
	#In case of failure None is returned
	def attach_iam_policy(self, role_name, policy_name, policy_document):
		ret = {}

		iam = boto3.resource('iam')

		try:
			role_policy = iam.RolePolicy(role_name, policy_name)
			ret = role_policy.put( PolicyDocument= json.dumps(policy_document)  )
		except Exception as e:
			self._logging.create_log( "error", f"Exception in attach_iam_policy:\n{e}")
			return None
		return ret

if __name__ == "__main__":
	irm = iamRoleManager()

	role_name = "transcoder_role"
	policy_name = "transcoder_role_policy"
	role_description = "Role for creating pipeline for transcoding pipeline"
	trust_document = {
	    "Version": "2012-10-17",
	    "Statement": [
	        {
	        "Effect": "Allow",
	        "Principal": {
	            "Service": "elastictranscoder.amazonaws.com"
	        },
	        "Action": "sts:AssumeRole"
	        }
	    ]
	}

	role_policy = {"Version":"2008-10-17","Statement":[{"Sid":"1","Effect":"Allow","Action":["s3:Put*","s3:ListBucket","s3:*MultipartUpload*","s3:Get*"],"Resource":"*"},{"Sid":"2","Effect":"Allow","Action":"sns:Publish","Resource":"*"},{"Sid":"3","Effect":"Deny","Action":["s3:*Delete*","s3:*Policy*","sns:*Remove*","sns:*Delete*","sns:*Permission*"],"Resource":"*"}]}

	resp = irm.create_iam_role(role_name, role_description, trust_document)
	print( f"Response Create Iam Role: \n{resp} \n")

	resp = irm.attach_iam_policy(role_name, policy_name, role_policy)
	print( f"Response Attach Iam Policy: \n{resp} \n")