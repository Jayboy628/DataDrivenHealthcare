# fetch_ssm_parameters.py
import boto3
import os

def fetch_ssm_parameters():
    ssm = boto3.client('ssm')
    parameters = ssm.describe_parameters()['Parameters']
    for param in parameters:
        name = param['Name']
        value = ssm.get_parameter(Name=name, WithDecryption=True)['Parameter']['Value']
        os.environ[name.split('/')[-1]] = value

if __name__ == "__main__":
    fetch_ssm_parameters()
