import csv
import errno
import json
import logging.config
import ntpath
import os
import random
import string
import sys
import traceback
from os import path
from shutil import copyfile

import boto3
import psycopg2 as psycopg2
import docker
import time
import json

abspath = os.path.abspath(os.path.dirname(__file__))
cadre = os.path.dirname(abspath)
util = cadre + '/util'
conf = cadre + '/conf'
sys.path.append(cadre)

import util.config_reader
import util.tool_util
from util.db_util import cadre_meta_connection_pool

log_conf = conf + '/logging-tool-conf.json'
with open(log_conf, 'r') as logging_configuration_file:
    config_dict = json.load(logging_configuration_file)

logging.config.dictConfig(config_dict)

# Log that the logger was configured
logger = logging.getLogger(__name__)
logger.info('Completed configuring logger()!')


logger = logging.getLogger('cadre_tool_listener')

# Create SQS client
tool_sqs_client = boto3.client('sqs',
                               aws_access_key_id=util.config_reader.get_aws_access_key(),
                               aws_secret_access_key=util.config_reader.get_aws_access_key_secret(),
                               region_name=util.config_reader.get_aws_region())

tool_queue_url = util.config_reader.get_tool_queue_url()


def upload_image_dockerhub(tool_name,
                           tool_id,
                           docker_path):
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    tool_name = tool_name.replace(" ", "")
    # We are building the docker image from the dockerfile here
    logger.info(tool_name)
    logger.info(tool_id)
    logger.info(docker_path)

    client.images.build(path=docker_path, tag=tool_name, forcerm=True)
    logger.info('Image built successfully')
    image = client.images.get(tool_name)
    docker_repo = util.config_reader.get_cadre_dockerhub_repo()
    image.tag(docker_repo, tag=tool_id)
    auth_config_payload = {'username': util.config_reader.get_cadre_dockerhub_username(), 'password': util.config_reader.get_cadre_dockerhub_pwd()}
    for line in client.images.push(docker_repo, stream=True, decode=True,auth_config=auth_config_payload):
        logger.info(line)
    client.images.remove(image=image.id, force=True)
    pruned_images = client.images.prune()


def poll_queue():
    while True:
        # Receive message from SQS queue
        response = tool_sqs_client.receive_message(
            QueueUrl=tool_queue_url,
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
            meta_connection = cadre_meta_connection_pool.getconn()
            meta_db_cursor = meta_connection.cursor()

            for message in response['Messages']:
                receipt_handle = message['ReceiptHandle']
                try:
                    message_body = message['Body']
                    logger.info("Received message id " + message['MessageId'])
                    query_json = json.loads(message_body)
                    logger.info(query_json)

                    job_id = query_json['job_id']
                    tool_id = query_json['tool_id']
                    username = query_json['username']
                    user_id = query_json['user_id']
                    tool_name = query_json['name']
                    description = query_json['description']
                    install_commands = query_json['install_commands']
                    file_paths = query_json['file_paths']
                    entrypoint_script = query_json['entrypoint']
                    environment = query_json['environment']

                    if 'python' is environment:
                        command = 'python'
                    else:
                        command = environment

                    copy_files = []
                    for file_path in file_paths:
                        file_info = {'name': file_path}
                        copy_files.append(file_info)
                    install_commands_list = []
                    if len(install_commands) > 0:
                        if ',' in install_commands:
                            commands_list = install_commands.split(",")
                            for command in commands_list:
                                command_info = {'name': command}
                                install_commands_list.append(command_info)
                        else:
                            install_commands_list = [{'name': install_commands}]                    
                    # create dockerfile
                    docker_template_json = {
                        'copy_files': copy_files,
                        'commands': install_commands_list,
                        'entrypoint': entrypoint_script
                    }
                    logger.info(docker_template_json)
                    util.tool_util.create_python_dockerfile_and_upload_s3(tool_id, docker_template_json)
                    logger.info('Dockerfile created and uploaded to S3')
                    # upload tools
                    util.tool_util.upload_tool_scripts_to_s3(file_paths, tool_id, username)
                    logger.info('Tool scripts uploaded to S3')

                    insert_q = "INSERT INTO tool(tool_id,description, name, script_name, command, created_on, created_by) VALUES (%s,%s,%s,%s,%s,NOW(),%s)"
                    data = (tool_id, description, tool_name, entrypoint_script, command, user_id)
                    meta_db_cursor.execute(insert_q, data)
                    meta_connection.commit()

                    # download tool scripts and dockerfile from s3 to efs/tools
                    docker_s3_root = util.config_reader.get_tools_s3_root()
                    efs_root = util.config_reader.get_cadre_efs_root_query_results_listener()
                    efs_subpath = util.config_reader.get_cadre_efs_subpath_query_results_listener()
                    efs_path = efs_root + efs_subpath
                    efs_tool_dir = efs_path + '/tools/' + tool_id
                    if not os.path.exists(efs_tool_dir):
                        os.makedirs(efs_tool_dir)
                    logger.info(docker_s3_root)    
                    logger.info(tool_id)    
                    logger.info(efs_tool_dir)    
                    util.tool_util.download_s3_dir(docker_s3_root, tool_id, efs_tool_dir)
                    # upload the image to dockerhub
                    upload_image_dockerhub(tool_name, tool_id, efs_tool_dir)

                    print("Job ID: " + job_id)
                    update_statement = "UPDATE user_job SET job_status = 'COMPLETED', modified_on = CURRENT_TIMESTAMP WHERE job_id = (%s)"
                    # Execute the SQL Query
                    meta_db_cursor.execute(update_statement, (job_id,))
                    meta_connection.commit()
                except (Exception, psycopg2.Error) as error:
                    traceback.print_tb(error.__traceback__)
                    logger.error('Error while connecting to PostgreSQL. Error is ' + str(error))
                finally:
                    # Closing database connection.
                    meta_db_cursor.close()
                    # Use this method to release the connection object and send back ti connection pool
                    cadre_meta_connection_pool.putconn(meta_connection)
                    logger.info("PostgreSQL connection pool is closed")
                    # Delete received message from queue
                    tool_sqs_client.delete_message(
                        QueueUrl=tool_queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    logger.info('Received and deleted message: %s' % message)
