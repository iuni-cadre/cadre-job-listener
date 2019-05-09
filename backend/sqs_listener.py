import json
import logging.config
import os
import sys
from os import path

import boto3

abspath = os.path.abspath(os.path.dirname(__file__))
cadre = os.path.dirname(abspath)
util = cadre + '/util'
conf = cadre + '/conf'
sys.path.append(cadre)

import util.config_reader

# If applicable, delete the existing log file to generate a fresh log file during each execution
logfile_path = cadre + "/cadre_job_listener.log"
if path.isfile(logfile_path):
    os.remove(logfile_path)

log_conf = conf + '/logging-conf.json'
with open(log_conf, 'r') as logging_configuration_file:
    config_dict = json.load(logging_configuration_file)

logging.config.dictConfig(config_dict)

# Log that the logger was configured
logger = logging.getLogger(__name__)
logger.info('Completed configuring logger()!')


logger = logging.getLogger('cadre_job_listener')

# Create SQS client
sqs_client = boto3.client('sqs',
                    aws_access_key_id=util.config_reader.get_aws_access_key(),
                    aws_secret_access_key=util.config_reader.get_aws_access_key_secret(),
                    region_name=util.config_reader.get_aws_region())

queue_url = util.config_reader.get_aws_queue_url()

while True:
    # Receive message from SQS queue
    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'All'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=300,
        WaitTimeSeconds=0
    )

    if 'Messages' in response:
        for message in response['Messages']:
            receipt_handle = message['ReceiptHandle']
            message_body = message['Body']
            logger.info("Received message id " + message['MessageId'])

            # Delete received message from queue
            sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.info('Received and deleted message: %s' % message)
