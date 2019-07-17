import boto3
import os
import configparser

config = configparser.ConfigParser() 
config.read(os.getcwd() + '/secrets.ini')
config.sections()
access_key = config['aws_credentials']['aws_access_key_id']
secret_access_key = config['aws_credentials']['aws_secret_access_key']

client = boto3.client(
  's3',
  aws_access_key_id=access_key,
  aws_secret_access_key=secret_access_key
)

response = client.list_objects_v2(
  Bucket='udacity-dend'
)

print(response['Contents'])