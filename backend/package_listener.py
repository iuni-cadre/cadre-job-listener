import csv
import errno
import json
import logging.config
import ntpath
import os
import sys
import traceback
from os import path
from shutil import copyfile

import boto3
import psycopg2 as psycopg2
import docker
import time

abspath = os.path.abspath(os.path.dirname(__file__))
cadre = os.path.dirname(abspath)
util = cadre + '/util'
conf = cadre + '/conf'
sys.path.append(cadre)

import util.config_reader
from util.db_util import cadre_meta_connection_pool

# If applicable, delete the existing log file to generate a fresh log file during each execution
logfile_path = cadre + "/cadre_package_listener.log"
if path.isfile(logfile_path):
    os.remove(logfile_path)

log_conf = conf + '/logging-package-conf.json'
with open(log_conf, 'r') as logging_configuration_file:
    config_dict = json.load(logging_configuration_file)

logging.config.dictConfig(config_dict)

# Log that the logger was configured
logger = logging.getLogger(__name__)
logger.info('Completed configuring logger()!')


logger = logging.getLogger('cadre_package_listener')

# Create SQS client
package_sqs_client = boto3.client('sqs',
                                  aws_access_key_id=util.config_reader.get_aws_access_key(),
                                  aws_secret_access_key=util.config_reader.get_aws_access_key_secret(),
                                  region_name=util.config_reader.get_aws_region())

package_queue_url = util.config_reader.get_package_queue_url()


def assert_dir_exists(path):
    """
    Checks if directory tree in path exists. If not it created them.
    :param path: the path to check if it exists
    """
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def download_s3_dir(client, bucket, path, target):
    """
    Downloads recursively the given S3 path to the target directory.
    :param client: S3 client to use.
    :param bucket: the name of the bucket to download from
    :param path: The S3 directory to download.
    :param target: the local directory to download the files to.
    """

    # Handle missing / at end of prefix
    if not path.endswith('/'):
        path += '/'

    paginator = client.meta.client.get_paginator('list_objects_v2')
    for result in paginator.paginate(Bucket=bucket, Prefix=path):
        # Download each file individually
        for key in result['Contents']:
            # Calculate relative path
            rel_path = key['Key'][len(path):]
            # Skip paths ending in /
            if not key['Key'].endswith('/'):
                local_file_path = os.path.join(target, rel_path)
                # Make sure directories exist
                local_file_dir = os.path.dirname(local_file_path)
                assert_dir_exists(local_file_dir)
                client.meta.client.download_file(bucket, key['Key'], local_file_path)


def run_docker_script(input_file_list, docker_path, tool_name, command, script_name):
    client = docker.DockerClient(base_url='tcp://127.0.0.1:2375')
    tool_name = tool_name.replace(" ", "")
    # We are building the docker image from the dockerfile here
    image = client.images.build(path=docker_path, tag=tool_name, forcerm=True)
    logger.info("The image has been built successfully. ")
    command_list = [command, script_name]
    volume = '/tmp'
    for input_file in input_file_list:
        file_name = ntpath.basename(input_file)
        file_name_for_image = volume + '/' + file_name
        copyfile(input_file, file_name_for_image)
        command_list.append(file_name_for_image)

    logger.info(command_list)

    container = client.containers.run(tool_name,
                                      detach=True,
                                      volumes={volume: {'bind': '/tmp/', 'mode': 'rw'}},
                                      command=command_list,
                                      remove=True)

    logger.info(container.logs())
    logger.info('The output of the file has been copied successfully outside the docker container')

    # Delete the docker container
    # client.containers.remove('sample_test', force=True)
    # print('The container has been removed successfully.')

    # Delete the docker image
    client.images.remove(tool_name, force=True)
    print('The image has been removed successfully.')

    # Deleting the unused images
    # args = {"dangling": True}
    client.images.prune()


def poll_queue():
    while True:
        # Receive message from SQS queue
        response = package_sqs_client.receive_message(
            QueueUrl=package_queue_url,
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
                    logger.info(message_body)
                    logger.info("Received message id " + message['MessageId'])
                    query_json = json.loads(message_body)
                    logger.info(query_json)

                    job_id = query_json['job_id']
                    package_id = query_json['package_id']
                    username = query_json['username']
                    output_file_names = query_json['output_filename']
                    logger.info("Job ID: " + job_id)
                    update_statement = "UPDATE user_job SET job_status = 'RUNNING', modified_on = CURRENT_TIMESTAMP WHERE job_id = (%s)"
                    # Execute the SQL Query
                    meta_db_cursor.execute(update_statement, (job_id,))
                    meta_connection.commit()
                    s3_client = boto3.resource('s3',
                                               aws_access_key_id=util.config_reader.get_aws_access_key(),
                                               aws_secret_access_key=util.config_reader.get_aws_access_key_secret(),
                                               region_name=util.config_reader.get_aws_region())
                    root_bucket_name = 'cadre-query-result'
                    bucket_location = username + '/packages/' + package_id + '/'
                    efs_root = util.config_reader.get_cadre_efs_root()
                    user_package_run_dir = efs_root + '/' + username + '/packages/' + package_id
                    if not os.path.exists(user_package_run_dir):
                        os.makedirs(user_package_run_dir)

                    # get package info
                    get_package_q = "SELECT a.s3_location, a.name from package p, archive a WHERE p.archive_id=a.archive_id AND p.package_id=%s"
                    meta_db_cursor.execute(get_package_q, (str(package_id),))
                    s3_archive_root = util.config_reader.get_archive_s3_root()

                    input_file_list = []
                    if meta_db_cursor.rowcount > 0:
                        input_files = meta_db_cursor.fetchall()
                        logger.info(input_files)
                        for input_file in input_files:
                            s3_location_root = input_file[0]
                            s3_archive_folder = s3_location_root[len(s3_archive_root) + 2:]
                            logger.info(s3_archive_folder)
                            s3_file_name = input_file[1]
                            input_copy = user_package_run_dir + '/' + s3_file_name
                            # download file from s3 and copy it to package_run dir in efs
                            folder_path = s3_archive_folder + '/' + s3_file_name
                            logger.info(folder_path)
                            s3_client.meta.client.download_file(s3_archive_root, folder_path, input_copy)
                            input_file_list.append(input_copy)

                    # get tool info
                    get_tool_q = "SELECT  t.name, t.tool_id, t.command, t.script_name FROM tool t, package p WHERE p.tool_id =t.tool_id AND p.package_id=%s"
                    meta_db_cursor.execute(get_tool_q, (package_id,))

                    if meta_db_cursor.rowcount > 0:
                        tool_info = meta_db_cursor.fetchone()
                        docker_s3_root = util.config_reader.get_tools_s3_root()
                        tool_name = tool_info[0]
                        tool_id = tool_info[1]
                        command = tool_info[2]
                        script_name = tool_info[3]
                        user_tool_dir = efs_root + '/' + username + '/tools/' + tool_id
                        if not os.path.exists(user_package_run_dir):
                            os.makedirs(user_tool_dir)
                        download_s3_dir(s3_client, docker_s3_root, tool_id, user_tool_dir)
                        run_docker_script(input_file_list,user_tool_dir, tool_name, command, script_name)
                    try:
                        for output_file in output_file_names:
                            ouptut_path = user_package_run_dir + '/' + output_file
                            logger.info(ouptut_path)
                            # convert_csv_to_json(ouptut_path, json_path, output_filter_string)
                            s3_client.meta.client.upload_file(ouptut_path, root_bucket_name,
                                                              bucket_location + output_file)
                    except:
                        print("Job ID: " + job_id)
                        update_statement = "UPDATE user_job SET job_status = 'FAILED', modified_on = CURRENT_TIMESTAMP WHERE job_id = (%s)"
                        # Execute the SQL Query
                        meta_db_cursor.execute(update_statement, (job_id,))
                        meta_connection.commit()

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
                    print("PostgreSQL connection pool is closed")
                    # Delete received message from queue
                    package_sqs_client.delete_message(
                        QueueUrl=package_queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    logger.info('Received and deleted message: %s' % message)