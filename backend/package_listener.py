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
from kubernetes import client, config, utils
from kubernetes.stream import stream
import kubernetes.client
from kubernetes.client.rest import ApiException

abspath = os.path.abspath(os.path.dirname(__file__))
cadre = os.path.dirname(abspath)
util = cadre + '/util'
conf = cadre + '/conf'
sys.path.append(cadre)

import util.config_reader
from util.db_util import cadre_meta_connection_pool

# If applicable, delete the existing log file to generate a fresh log file during each execution
# logfile_path = cadre + "/cadre_package_listener.log"
# if path.isfile(logfile_path):
#     os.remove(logfile_path)

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
kub_config_location = util.config_reader.get_kub_config_location()

config.load_kube_config(config_file=kub_config_location)
configuration = kubernetes.client.Configuration()
api_instance = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient(configuration))


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


def kube_delete_empty_pods(namespace, phase):
    """
    Pods are never empty, just completed the lifecycle.
    As such they can be deleted.
    Pods can be without any running container in 2 states:
    Succeeded and Failed. This call doesn't terminate Failed pods by default.
    """
    # The always needed object
    deleteoptions = client.V1DeleteOptions()
    # We need the api entry point for pods
    # List the pods
    try:
        pods = client.list_namespaced_pod(namespace,
                                            include_uninitialized=False,
                                            pretty=True,
                                            timeout_seconds=60)
    except ApiException as e:
        logging.error("Exception when calling CoreV1Api->list_namespaced_pod: %s\n" % e)

    for pod in pods.items:
        logging.info(pod)
        podname = pod.metadata.name
        try:
            if pod.status.phase == phase:
                api_response = api_pods.delete_namespaced_pod(podname, namespace, deleteoptions)
                logging.info("Pod: {} deleted!".format(podname))
                logging.info(api_response)
            else:
                logging.info("Pod: {} still not done... Phase: {}".format(podname, pod.status.phase))
        except ApiException as e:
            logging.error("Exception when calling CoreV1Api->delete_namespaced_pod: %s\n" % e)

    return


def kube_create_job_object(name,
                           image_name,
                           container_tag,
                           namespace,
                           container_name,
                           command,
                           script_name,
                           env_vars,
                           input_file_list,
                           volume_full_path,
                           volume_subpath):
    logger.info(image_name)
    """
    Create a k8 Job Object
    Minimum definition of a job object:
    {'api_version': None, - Str
    'kind': None,     - Str
    'metadata': None, - Metada Object
    'spec': None,     -V1JobSpec
    'status': None}   - V1Job Status
    Docs: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Job.md
    Docs2: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/#writing-a-job-spec
    Also docs are pretty pretty bad. Best way is to pip install kubernetes and go via the autogenerated code
    And figure out the chain of objects that you need to hold a final valid object So for a job object you need:
    V1Job -> V1ObjectMeta
          -> V1JobStatus
          -> V1JobSpec -> V1PodTemplate -> V1PodTemplateSpec -> V1Container

    Now the tricky part, is that V1Job.spec needs a .template, but not a PodTemplateSpec, as such
    you need to build a PodTemplate, add a template field (template.template) and make sure
    template.template.spec is now the PodSpec.
    Then, the V1Job.spec needs to be a JobSpec which has a template the template.template field of the PodTemplate.
    Failure to do so will trigger an API error.
    Also Containers must be a list!
    Docs3: https://github.com/kubernetes-client/python/issues/589
    """
    # Body is the object Body
    body = client.V1Job(api_version="batch/v1", kind="Job")
    # Body needs Metadata
    # Attention: Each JOB must have a different name!
    body.metadata = client.V1ObjectMeta(namespace=namespace, name=name)
    # And a Status
    body.status = client.V1JobStatus()
    # Now we start with the Template...
    template = client.V1PodTemplate()
    template.template = client.V1PodTemplateSpec()
    # Passing Arguments in Env:
    env_list = []
    for env_name, env_value in env_vars.items():
        env_list.append(client.V1EnvVar(name=env_name, value=env_value))

    shared_volume_in_pod = '/data'
    shared_volume = volume_full_path
    input_dir = shared_volume_in_pod + '/input_files'
    output_dir = shared_volume_in_pod + '/output_files'
    # input_dir = shared_volume + '/input'
    # if not os.path.exists(input_dir):
    #     os.makedirs(input_dir)
    #
    # output_dir = shared_volume + '/output1'
    # if not os.path.exists(output_dir):
    #     os.makedirs(output_dir)

    command_list = [command, script_name]
    inputs_as_string = ",".join(input_file_list)
    args = [inputs_as_string, input_dir, output_dir]
    logger.info(args)

    pod = client.V1Pod()

    pod.metadata = client.V1ObjectMeta(name=name)
    hostpathvolumesource = client.V1HostPathVolumeSource(path=shared_volume, type='DirectoryOrCreate')
    volume_spec = client.V1PersistentVolumeSpec(storage_class_name='', volume_mode='Filesystem',
                                                access_modes=['ReadWriteMany'], host_path=hostpathvolumesource,
                                                capacity={'storage': '2Gi'})
    pv_meta = client.V1ObjectMeta(name=util.config_reader.get_cadre_pv_name())
    persistent_volume = client.V1PersistentVolume(metadata=pv_meta, api_version='v1', kind='PersistentVolume',
                                                  spec=volume_spec)
    resource_requirements = client.V1ResourceRequirements(limits={'cpu': 2, 'memory': '80Mi', 'storage': '2Gi'},
                                                          requests={'cpu': 1, 'memory': '40Mi', 'storage': '1Gi'})
    claim_spec = client.V1PersistentVolumeClaimSpec(storage_class_name='', access_modes=['ReadWriteMany'],
                                                    resources=resource_requirements)
    pvc_meta = client.V1ObjectMeta(name=util.config_reader.get_cadre_pvc_name())
    persistent_volume_claim = client.V1PersistentVolumeClaim(api_version='v1', metadata=pvc_meta,
                                                             kind='PersistentVolumeClaim', spec=claim_spec)
    claim_volume_source = client.V1PersistentVolumeClaimVolumeSource(claim_name=util.config_reader.get_cadre_pvc_name())
    volume_mounts = [client.V1VolumeMount(mount_path=shared_volume_in_pod, name=util.config_reader.get_cadre_pv_name(), sub_path=volume_subpath)]

    image_with_tag = image_name + ":" + container_tag
    container = client.V1Container(name=container_name,
                                   image=image_with_tag,
                                   env=env_list,
                                   command=command_list,
                                   args=args,
                                   image_pull_policy='IfNotPresent')

    container.volume_mounts = volume_mounts
    docker_secret_name = util.config_reader.get_kub_docker_secret()
    secret_name = client.V1LocalObjectReference(name=docker_secret_name)
    # pod_meta = client.V1ObjectMeta(name='private-reg')
    spec = client.V1PodSpec(containers=[container], restart_policy='Never', image_pull_secrets=[secret_name], active_deadline_seconds=600)
    volume = client.V1Volume(name=util.config_reader.get_cadre_pv_name(), persistent_volume_claim=claim_volume_source)
    spec.volumes = [volume]
    # And finaly we can create our V1JobSpec!
    pod.spec = spec
    # body.spec = client.V1JobSpec(ttl_seconds_after_finished=600, template=template.template)
    return pod


def upload_image_dockerhub(docker_path,
                           tool_name,
                           package_id):
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    tool_name = tool_name.replace(" ", "")
    # We are building the docker image from the dockerfile here
    logger.info(tool_name)
    logger.info(package_id)
    client.images.build(path=docker_path, tag=tool_name)
    image = client.images.get(tool_name)
    docker_repo = util.config_reader.get_cadre_dockerhub_repo()
    image.tag(docker_repo, tag=package_id)
    auth_config_payload = {'username': util.config_reader.get_cadre_dockerhub_username(), 'password': util.config_reader.get_cadre_dockerhub_pwd()}
    for line in client.images.push(docker_repo, stream=True, decode=True,auth_config=auth_config_payload):
        logger.info(line)
    logger.info("The image has been built successfully. ")
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
                    logger.info("Job ID: " + job_id)

                    # Getting the package name from the package table in the cadre metadatabase
                    get_package_name = "SELECT name FROM package WHERE package_id=%s"
                    meta_db_cursor.execute(get_package_name, (package_id,))

                    package_name = package_id
                    if meta_db_cursor.rowcount > 0:
                        package_info = meta_db_cursor.fetchone()
                        package_name = package_info[0]
                        logger.info(package_name)

                    update_statement = "UPDATE user_job SET job_status = 'RUNNING', modified_on = CURRENT_TIMESTAMP WHERE job_id = (%s)"
                    # Execute the SQL Query
                    meta_db_cursor.execute(update_statement, (job_id,))
                    meta_connection.commit()
                    s3_client = boto3.resource('s3',
                                               aws_access_key_id=util.config_reader.get_aws_access_key(),
                                               aws_secret_access_key=util.config_reader.get_aws_access_key_secret(),
                                               region_name=util.config_reader.get_aws_region())
                    root_bucket_name = 'cadre-query-result'
                    bucket_location = username + '/packages/' + package_name + '/'
                    efs_root = util.config_reader.get_cadre_efs_root_query_results_listener()
                    efs_subpath = util.config_reader.get_cadre_efs_subpath_query_results_listener()
                    efs_path = efs_root + efs_subpath
                    user_package_run_dir = efs_path + '/' + username + '/packages/' + package_name
                    subpath = user_package_run_dir[len(efs_root):]
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
                            input_dir = user_package_run_dir + '/input_files/'
                            input_copy = input_dir + s3_file_name
                            if not os.path.exists(input_dir):
                                os.makedirs(input_dir)
                            # download file from s3 and copy it to package_run dir in efs
                            folder_path = s3_archive_folder + '/' + s3_file_name
                            logger.info(folder_path)
                            s3_client.meta.client.download_file(s3_archive_root, folder_path, input_copy)
                            input_file_list.append(s3_file_name)

                    # get tool info
                    get_tool_q = "SELECT  t.name, t.tool_id, t.command, t.script_name FROM tool t, package p WHERE p.tool_id =t.tool_id AND p.package_id=%s"
                    meta_db_cursor.execute(get_tool_q, (package_id,))

                    body = None
                    jhub_namespace = util.config_reader.get_kebenetes_namespace()
                    if meta_db_cursor.rowcount > 0:
                        tool_info = meta_db_cursor.fetchone()
                        docker_s3_root = util.config_reader.get_tools_s3_root()
                        tool_name = tool_info[0]
                        tool_name = tool_name.replace(" ", "")
                        logger.info(tool_name)
                        tool_id = tool_info[1]
                        command = tool_info[2]
                        logger.info(command)
                        script_name = tool_info[3]
                        user_tool_dir = user_package_run_dir + '/tools/' + tool_name
                        logger.info(user_tool_dir)
                        if not os.path.exists(user_tool_dir):
                            os.makedirs(user_tool_dir)
                        output_dir = user_package_run_dir + '/output_files/'
                        if not os.path.exists(output_dir):
                            os.makedirs(output_dir)
                        logger.info(output_dir)
                        download_s3_dir(s3_client, docker_s3_root, tool_id, user_tool_dir)
                        upload_image_dockerhub(user_tool_dir, tool_name, package_id)
                        job_name = job_id

                        image_name = util.config_reader.get_cadre_dockerhub_repo()
                        logger.info(image_name)
                        kube_delete_empty_pods(jhub_namespace, "Completed")
                        body = kube_create_job_object(job_name,
                                                      image_name,
                                                      package_id,
                                                      jhub_namespace,
                                                      tool_name,
                                                      command,
                                                      script_name,
                                                      env_vars={"VAR": "TESTING"},
                                                      input_file_list=input_file_list,
                                                      volume_full_path=user_package_run_dir,
                                                      volume_subpath=subpath)
                        try:
                            api_response = api_instance.create_namespaced_pod(jhub_namespace, body, pretty=True)
                            logger.info(api_response)
                        except ApiException as e:
                            logger.error("Exception when calling BatchV1Api->create_namespaced_job: %s\n" % e)
                            update_statement = "UPDATE user_job SET job_status = 'FAILED', modified_on = CURRENT_TIMESTAMP WHERE job_id = (%s)"
                            # Execute the SQL Query
                            meta_db_cursor.execute(update_statement, (job_id,))
                            meta_connection.commit()
                    try:
                        for output_file in output_file_names:
                            output_file = output_file.replace(" ", "")
                            output_path = output_dir + output_file
                            # convert_csv_to_json(output_dir, json_path, output_filter_string)
                            s3_client.meta.client.upload_file(output_path, root_bucket_name,
                                                              bucket_location + output_file)
                    except:
                        logger.info("Job ID: " + job_id)
                        update_statement = "UPDATE user_job SET job_status = 'FAILED', modified_on = CURRENT_TIMESTAMP WHERE job_id = (%s)"
                        # Execute the SQL Query
                        meta_db_cursor.execute(update_statement, (job_id,))
                        meta_connection.commit()

                    print("Job ID: " + job_id)
                    update_statement = "UPDATE user_job SET job_status = 'COMPLETED', modified_on = CURRENT_TIMESTAMP WHERE job_id = (%s)"
                    # Execute the SQL Query
                    meta_db_cursor.execute(update_statement, (job_id,))
                    meta_connection.commit()

                    # if body is not None:
                    #     api_response = api_instance.delete_namespaced_pod(name=job_id,
                    #                                                       namespace=jhub_namespace,
                    #                                                       body=body)
                    #     logger.info(api_response)
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
                    package_sqs_client.delete_message(
                        QueueUrl=package_queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    logger.info('Received and deleted message: %s' % message)