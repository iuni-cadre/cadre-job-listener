import csv
import json
import logging.config
import os
import sys
import traceback
from os import path

import boto3
import psycopg2 as psycopg2
import time

from neo4j import GraphDatabase

abspath = os.path.abspath(os.path.dirname(__file__))
cadre = os.path.dirname(abspath)
util = cadre + '/util'
conf = cadre + '/conf'
sys.path.append(cadre)

import util.config_reader
from util.db_util import wos_connection_pool, mag_connection_pool, cadre_meta_connection_pool, mag_driver

# If applicable, delete the existing log file to generate a fresh log file during each execution
logfile_path = cadre + "/cadre_job_listener.log"
if path.isfile(logfile_path):
    os.remove(logfile_path)

log_conf = conf + '/logging-job-conf.json'
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

queue_url = util.config_reader.get_job_queue_url()


def generate_wos_query(output_filter_string, query_json):
    interface_query = 'SELECT ' + output_filter_string + ' FROM wos_core.interface_table WHERE '
    for item in query_json:
        if 'value' in item:
            value = item['value']
        if 'operation' in item:
            operand = item['operation']
        if 'field' in item:
            field = item['field']
            if field == 'year':
                if value is not None:
                    value = value.strip()
                    if len(value) == 4 and value.isdigit():
                        value = "'{}'".format(value)
                        logger.info("Year: " + value)
                        interface_query += ' year={} '.format(value) + operand
                        # years.append(value)
                        # year_operands.append(operand)
            elif field == 'journals_name':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value + '%'
                    value = "'{}'".format(value)
                    logger.info("Journals Name: " + value)
                    interface_query += ' journal_tsv @@ to_tsquery ({}) '.format(value) + operand
                    # journals.append(value)
                    # journal_operands.append(operand)
            elif field == 'authors_full_name':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value + '%'
                    value = "'{}'".format(value)
                    logger.info("authors_full_name: " + value)
                    interface_query += ' authors_full_name iLIKE {} '.format(value) + operand
                    # authors.append(value)
            elif field == 'title':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value + '%'
                    value = "'{}'".format(value)
                    logger.info("Title: " + value)
                    interface_query += ' title_tsv @@ to_tsquery ({}) '.format(value) + operand
                    # authors.append(value)

    interface_query = interface_query + 'LIMIT' + ' ' + '10000'
    print("Query: " + interface_query)
    return interface_query


def generate_mag_query(output_filter_string, query_json):
    logger.info(output_filter_string)
    logger.info(query_json)
    interface_query = 'SELECT ' + output_filter_string + ' FROM mag_core.mag_interface_table WHERE '
    for item in query_json:
        if 'value' in item:
            value = item['value']
        if 'operation' in item:
            operand = item['operation']
        if 'field' in item:
            field = item['field']
            if field == 'year':
                if value is not None:
                    value = value.strip()
                    if len(value) == 4 and value.isdigit():
                        value = "'{}'".format(value)
                        print("Year: " + value)
                        interface_query += ' year={} '.format(value) + operand
                        # years.append(value)
                        # year_operands.append(operand)
            elif field == 'journal_display_name':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value + '%'
                    value = "'{}'".format(value)
                    print("Journals Name: " + value)
                    interface_query += ' journal_display_name iLIKE {} '.format(value) + operand
                    # journals.append(value)
                    # journal_operands.append(operand)
            elif field == 'book_title':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value.upper() + '%'
                    logger.info('Book Title: ' + value)
                    interface_query += ' book_title iLIKE {} '.format(value) + operand
            elif field == 'doi':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value.upper() + '%'
                    logger.info('DOI: ' + value)
                    interface_query += ' doi iLIKE {} '.format(value) + operand
            elif field == 'conference_display_name':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value.upper() + '%'
                    logger.info('conferenceDisplayName: ' + value)
                    interface_query += ' conference_display_name iLIKE {} '.format(value) + operand
            elif field == 'paper_title':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value + '%'
                    value = "'{}'".format(value)
                    print("Title: " + value)
                    interface_query += ' paper_title_tsv @@ to_tsquery ({}) '.format(value) + operand
                    # authors.append(value)

    interface_query = interface_query + 'LIMIT' + ' ' + '10000'
    print("Query: " + interface_query)
    return interface_query


def convert_csv_to_json(csv_path, json_path, output_filter_string):
    csvfile = open(csv_path, 'r')
    jsonfile = open(json_path, 'w')

    fieldnames = tuple(output_filter_string.split(','))
    reader = csv.DictReader(csvfile, fieldnames)
    for row in reader:
        json.dump(row, jsonfile)
        jsonfile.write('\n')


def poll_queue():
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
            wos_connection = wos_connection_pool.getconn()
            wos_cursor = wos_connection.cursor()
            mag_connection = mag_connection_pool.getconn()
            mag_cursor = mag_connection.cursor()
            meta_connection = cadre_meta_connection_pool.getconn()
            meta_db_cursor = meta_connection.cursor()
            output_filters_single = []

            for message in response['Messages']:
                receipt_handle = message['ReceiptHandle']
                try:
                    message_body = message['Body']
                    logger.info(message_body)
                    logger.info("Received message id " + message['MessageId'])
                    query_json = json.loads(message_body)
                    logger.info(query_json)

                    network_query_type = 'other'
                    degree = 0
                    dataset = query_json['dataset']
                    filters = query_json['filters']
                    job_id = query_json['job_id']
                    username = query_json['username']
                    output_fields = query_json['output']

                    for output_filed in output_fields:
                        type = output_filed['type']
                        if type == 'single':
                            field = output_filed['field']
                            output_filters_single.append(field)
                        else:
                            network_query_type = output_filed['field']
                            degree = int(output_filed['degree'])
                            output_filters_single.append('paper_id')
                    output_filter_string = ",".join(output_filters_single)
                    # Updating the job status in the job database as running
                    print("Job ID: " + job_id)
                    updateStatement = "UPDATE user_job SET job_status = 'RUNNING', modified_on = CURRENT_TIMESTAMP WHERE job_id = (%s)"
                    # Execute the SQL Query
                    meta_db_cursor.execute(updateStatement, (job_id,))
                    print(meta_connection.get_dsn_parameters())
                    meta_connection.commit()
                    s3_client = boto3.resource('s3',
                                               aws_access_key_id=util.config_reader.get_aws_access_key(),
                                               aws_secret_access_key=util.config_reader.get_aws_access_key_secret(),
                                               region_name=util.config_reader.get_aws_region())
                    root_bucket_name = 'cadre-query-result'
                    bucket_location = username + '/query-results/'

                    # Generating the Query that needs to run on the RDS
                    try:
                        efs_root = util.config_reader.get_cadre_efs_root()
                        user_query_result_dir = efs_root + '/' + username + '/query-results/'
                        if not os.path.exists(user_query_result_dir):
                            os.makedirs(user_query_result_dir)
                        csv_path = user_query_result_dir + job_id + '.csv'
                        logger.info(csv_path)
                        logger.info(dataset)
                        if dataset == 'wos':
                            logger.info('User selects WOS dataset !!!')
                            if network_query_type == 'citation':
                                output_filter_string = 'paper_reference_id'
                                interface_query = generate_wos_query(output_filter_string, filters)
                            else:
                                interface_query = generate_wos_query(output_filter_string, filters)
                                logger.info(interface_query)
                                output_query = "COPY ({}) TO STDOUT WITH CSV HEADER".format(interface_query)
                                with open(csv_path, 'w') as f:
                                    wos_cursor.copy_expert(output_query, f)
                        else:
                            logger.info('User selects MAG dataset !!!')
                            if network_query_type == 'citation':
                                output_filter_string = 'paper_id'
                                interface_query = generate_mag_query(output_filter_string, filters)
                                with mag_driver.session() as session:
                                    neo4j_query = "CALL apoc.export.json.query(\"CALL apoc.load.jdbc('postgresql_url'," \
                                                  " ' " + interface_query + "') YIELD row MATCH (n:paper)<-[*2]-(m:paper)" \
                                                                            " WHERE n.paper_id = row.paper_id RETURN n, m\", '" + csv_path +  "')"
                                    result = session.run(neo4j_query)
                                    logger.info(result)
                            else:
                                interface_query = generate_mag_query(output_filter_string, filters)
                                logger.info(interface_query)
                                output_query = "COPY ({}) TO STDOUT WITH CSV HEADER".format(interface_query)
                                with open(csv_path, 'w') as f:
                                    mag_cursor.copy_expert(output_query, f)
                                # convert_csv_to_json(csv_path, json_path, output_filter_string)
                        s3_client.meta.client.upload_file(csv_path, root_bucket_name,
                                                          bucket_location + job_id + '.csv')
                    except:
                        print("Job ID: " + job_id)
                        updateStatement = "UPDATE user_job SET job_status = 'FAILED', modified_on = CURRENT_TIMESTAMP WHERE job_id = (%s)"
                        # Execute the SQL Query
                        meta_db_cursor.execute(updateStatement, (job_id,))
                        meta_connection.commit()

                    print("Job ID: " + job_id)
                    updateStatement = "UPDATE user_job SET job_status = 'COMPLETED', modified_on = CURRENT_TIMESTAMP WHERE job_id = (%s)"
                    # Execute the SQL Query
                    meta_db_cursor.execute(updateStatement, (job_id,))
                    meta_connection.commit()
                except (Exception, psycopg2.Error) as error:
                    traceback.print_tb(error.__traceback__)
                    logger.error('Error while connecting to PostgreSQL. Error is ' + str(error))
                finally:
                    # Closing database connection.
                    wos_cursor.close()
                    mag_cursor.close()
                    meta_db_cursor.close()
                    # Use this method to release the connection object and send back ti connection pool
                    wos_connection_pool.putconn(wos_connection)
                    mag_connection_pool.putconn(mag_connection)
                    cadre_meta_connection_pool.putconn(meta_connection)
                    print("PostgreSQL connection pool is closed")
                    # Delete received message from queue
                    sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    logger.info('Received and deleted message: %s' % message)
