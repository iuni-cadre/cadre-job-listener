import os
import sys
import traceback
import logging

import boto3
import jinja2

abspath = os.path.abspath(os.path.dirname(__file__))
cadre = os.path.dirname(abspath)
util = cadre + '/util'
conf = cadre + '/conf'
sys.path.append(cadre)
logger = logging.getLogger(__name__)

import util.config_reader

s3_file_archive = util.config_reader.get_archive_s3_root()
s3_tool_location = util.config_reader.get_tools_s3_root()
aws_access_key_id = util.config_reader.get_aws_access_key()
aws_secret = util.config_reader.get_aws_access_key_secret()
aws_region = util.config_reader.get_aws_region()
efs_path = util.config_reader.get_cadre_efs_root_query_results_listener() + util.config_reader.get_cadre_efs_subpath_query_results_listener()


def create_python_dockerfile_and_upload_s3(tool_id, docker_template_json):
    try:
        s3_client = boto3.resource('s3',
                                   aws_access_key_id=aws_access_key_id,
                                   aws_secret_access_key=aws_secret,
                                   region_name=aws_region)
        template_loader = jinja2.FileSystemLoader(searchpath=conf_path)
        template_env = jinja2.Environment(loader=template_loader)
        TEMPLATE_FILE = "python3.7_dockerfile_template"
        template = template_env.get_template(TEMPLATE_FILE)
        dockerfile_content = template.render(docker_info=docker_template_json)  # this is where to put args to the template renderer
        dockerfile_s3_subpath = tool_id + "/Dockerfile"
        s3_client.Object(s3_tool_location, dockerfile_s3_subpath).put(Body=dockerfile_content)
    except (Exception) as error:
        traceback.print_tb(error.__traceback__)
        print("Error while archiving files to s3")


# archive files to s3
# file paths are relative to users home directory
# need to get the efs home from config
def archive_input_files(files, username):
    try:
        s3_client = boto3.resource('s3',
                                   aws_access_key_id=aws_access_key_id,
                                   aws_secret_access_key=aws_secret,
                                   region_name=aws_region)

        for file in files:
            file_full_path = efs_path + '/' + username + '/' + file
            logger.info(file_full_path)
            filename = os.path.basename(file_full_path)
            s3_archive_sub_path = username + '/' + filename
            s3_client.meta.client.upload_file(file_full_path, s3_file_archive, s3_archive_sub_path)
    except (Exception) as error:
        traceback.print_tb(error.__traceback__)
        print("Error while archiving files to s3")


# upload tool script files to s3 tool location
# file paths are relative to users home directory
# need to get the efs home from config
def upload_tool_scripts_to_s3(files, tool_id):
    try:
        s3_client = boto3.resource('s3',
                                   aws_access_key_id=aws_access_key_id,
                                   aws_secret_access_key=aws_secret,
                                   region_name=aws_region)

        for file in files:
            file_full_path = efs_path + '/' + username + '/' + file
            logger.info(file_full_path)
            filename = os.path.basename(file_full_path)
            s3_tool_sub_path = tool_id + '/' + filename
            s3_client.meta.client.upload_file(file_full_path, s3_tool_location, s3_tool_sub_path)
    except (Exception) as error:
        traceback.print_tb(error.__traceback__)
        print("Error while uploading files to s3 tool location")


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


def download_s3_dir(bucket, path, target):
    """
    Downloads recursively the given S3 path to the target directory.
    :param bucket: the name of the bucket to download from
    :param path: The S3 directory to download.
    :param target: the local directory to download the files to.
    """
    try:
        s3_client = boto3.resource('s3',
                                   aws_access_key_id=aws_access_key_id,
                                   aws_secret_access_key=aws_secret,
                                   region_name=aws_region)
        # Handle missing / at end of prefix
        if not path.endswith('/'):
            path += '/'

        paginator = s3_client.meta.client.get_paginator('list_objects_v2')
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
    except (Exception) as error:
        traceback.print_tb(error.__traceback__)
        print("Error while downloading files from s3 tool location to EFS")