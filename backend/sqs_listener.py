import json
import logging.config
import os
import sys
import traceback
from os import path

import boto3
import psycopg2 as psycopg2

abspath = os.path.abspath(os.path.dirname(__file__))
cadre = os.path.dirname(abspath)
util = cadre + '/util'
conf = cadre + '/conf'
sys.path.append(cadre)

import util.config_reader
from util.db_util import wos_connection_pool, cadre_meta_connection_pool

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
        wos_connection = wos_connection_pool.getconn()
        wos_cursor = wos_connection.cursor()
        meta_connection = cadre_meta_connection_pool.getconn()
        meta_db_cursor = meta_connection.cursor()
        try:
            for message in response['Messages']:
                receipt_handle = message['ReceiptHandle']
                message_body = message['Body']
                logger.info(message_body)
                logger.info("Received message id " + message['MessageId'])

                query_json = json.loads(message_body)

                logger.info(query_json)

                # extract the job id from the message
                username = ''
                job_id = ''
                for item in query_json:
                    if 'job_id' in item:
                        job_id = item['job_id']
                    if 'username' in item:
                        username = item['username']

                # Updating the job status in the job database as running

                # Use getconn() method to Get Connection from connection pool from the job database

                print("Job ID: " + job_id)
                updateStatement = "UPDATE user_job SET job_status = 'RUNNING', last_updated = CURRENT_TIMESTAMP WHERE j_id = (%s)"
                # Execute the SQL Query
                meta_db_cursor.execute(updateStatement, (job_id,))
                print(meta_connection.get_dsn_parameters())
                meta_connection.commit()

                # Generating the Query that needs to run on the RDS

                value_array = []
                interface_query = 'SELECT wos_id, year, number, issue, pages, authors_full_name, authors_id_orcid, ' \
                                  'authors_id_dais, authors_id_research, authors_prefix, authors_first_name, authors_middle_name, ' \
                                  'authors_last_name, authors_suffix, authors_initials, authors_display_name, authors_wos_name, ' \
                                  'authors_id_lang, authors_email, "references", issn, doi, title, journal_name, journal_abbrev, ' \
                                  'journal_iso, abstract_paragraphs FROM interface_table WHERE '
                for item in query_json:
                    if 'value' in item:
                        value = item['value']
                    if 'operand' in item:
                        operand = item['operand']
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
                        elif field == 'journalsName':
                            if value is not None:
                                value = value.strip()
                                value = value.replace(' ', '%')
                                value = '%' + value + '%'
                                value = "'{}'".format(value)
                                print("Journals Name: " + value)
                                interface_query += ' journal_tsv @@ to_tsquery ({}) '.format(value) + operand
                                # journals.append(value)
                                # journal_operands.append(operand)
                        elif field == 'authorsFullName':
                            if value is not None:
                                value = value.strip()
                                value = value.replace(' ', '%')
                                value = '%' + value + '%'
                                value = "'{}'".format(value)
                                print("Authors Full Name: " + value)
                                interface_query += ' authors_full_name iLIKE {} '.format(value) + operand
                                # authors.append(value)
                        elif field == 'title':
                            if value is not None:
                                value = value.strip()
                                value = value.replace(' ', '%')
                                value = '%' + value + '%'
                                value = "'{}'".format(value)
                                print("Title: " + value)
                                interface_query += ' title_tsv @@ to_tsquery ({}) '.format(value) + operand
                                # authors.append(value)

                interface_query = interface_query + 'LIMIT' + ' ' + '9999'
                print("Query: " + interface_query)

                print("Job ID: " + job_id)

                try:
                    output_query = "COPY ({}) TO STDOUT WITH CSV HEADER".format(interface_query)
                    path = util.config_reader.get_cadre_efs_root() + '/' + username + '/' + job_id + '.csv'
                    with open(path, 'w') as f:
                        wos_cursor.copy_expert(output_query, f)

                    s3_client = boto3.resource('s3',
                                               aws_access_key_id=util.config_reader.get_aws_access_key(),
                                               aws_secret_access_key=util.config_reader.get_aws_access_key_secret(),
                                               region_name=util.config_reader.get_aws_region())
                    root_bucket_name = "cadre-query-result"
                    bucket_job_id = '{}/'.format(job_id)
                    print("Bucket Job ID: " + bucket_job_id)
                    s3_location = 's3://' + bucket_job_id
                    s3_client.meta.client.upload_file(path, root_bucket_name, bucket_job_id + job_id + '.csv')
                except:
                    print("Job ID: " + job_id)
                    updateStatement = "UPDATE user_job SET job_status = 'FAILED', last_updated = CURRENT_TIMESTAMP WHERE j_id = (%s)"
                    # Execute the SQL Query
                    meta_db_cursor.execute(updateStatement, (job_id,))
                    meta_connection.commit()

                print("Job ID: " + job_id)
                updateStatement = "UPDATE user_job SET job_status = 'COMPLETED', last_updated = CURRENT_TIMESTAMP WHERE j_id = (%s)"
                # Execute the SQL Query
                meta_db_cursor.execute(updateStatement, (job_id,))
                meta_connection.commit()

                # Delete received message from queue
                sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )
                logger.info('Received and deleted message: %s' % message)
        except (Exception, psycopg2.Error) as error:
            traceback.print_tb(error.__traceback__)
            logger.error('Error while connecting to PostgreSQL. Error is ' + str(error))
        finally:
            # Closing database connection.
            wos_cursor.close()
            meta_db_cursor.close()
            # Use this method to release the connection object and send back ti connection pool
            wos_connection_pool.putconn(wos_connection)
            cadre_meta_connection_pool.putconn(meta_connection)
            print("PostgreSQL connection pool is closed")
