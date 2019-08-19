import csv
import json
import logging.config
import os
import shutil
import sys
import traceback
from os import path

import boto3
import psycopg2 as psycopg2
import time
from shutil import copyfile

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


def generate_wos_query(output_filter_string, query_json, network_enabled):
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
                        if network_enabled:
                            value = value.replace("'", "\\'")
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
                    if network_enabled:
                        value = value.replace("'", "\\'")
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
                    if network_enabled:
                        value = value.replace("'", "\\'")
                    logger.info("authors_full_name: " + value)
                    interface_query += ' authors_full_name iLIKE {} '.format(value) + operand
                    # authors.append(value)
            elif field == 'title':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value + '%'
                    value = "'{}'".format(value)
                    if network_enabled:
                        value = value.replace("'", "\\'")
                    logger.info("Title: " + value)
                    interface_query += ' title_tsv @@ to_tsquery ({}) '.format(value) + operand
                    # authors.append(value)

    if network_enabled:
        interface_query = interface_query + 'LIMIT' + ' ' + '1000'
    else:
        interface_query = interface_query + 'LIMIT' + ' ' + '10000'
    print("Query: " + interface_query)
    return interface_query


def generate_mag_query(output_filter_string, query_json, network_enabled):
    logger.info(output_filter_string)
    logger.info(query_json)
    if network_enabled:
        output_filter_string = "paper_id, author_id, author_sequence_number::varchar," \
                  "authors_display_name,authors_last_known_affiliation_id,journal_id,conference_series_id,conference_instance_id,"\
                  "paper_reference_id,field_of_study_id,doi,doc_type,paper_title,original_title,book_title,year,date::varchar,"\
                  "paper_publisher,issue,paper_abstract,paper_first_page,paper_last_page,paper_reference_count::varchar,"\
                  "paper_citation_count::varchar,paper_estimated_citation::varchar,conference_display_name,journal_display_name,"\
                  "journal_issn,journal_publisher"
    interface_query = 'SELECT ' + output_filter_string + ' FROM mag_core.final_mag_interface_table WHERE '
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
                        if network_enabled:
                            value = value.replace("'", "\\'")
                        logger.info("Year: " + value)
                        interface_query += ' year={} '.format(value) + operand
                        # years.append(value)
                        # year_operands.append(operand)
            elif field == 'journal_display_name':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value + '%'
                    value = "'{}'".format(value)
                    if network_enabled:
                        value = value.replace("'", "\\'")
                    logger.info("Journals Name: " + value)
                    interface_query += ' journal_display_name iLIKE {} '.format(value) + operand
                    # journals.append(value)
                    # journal_operands.append(operand)
            elif field == 'authors_display_name':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value.upper() + '%'
                    value = "'{}'".format(value)
                    if network_enabled:
                        value = value.replace("'", "\\'")
                    logger.info('Authors Name: ' + value)
                    interface_query += ' authors_display_name iLIKE {} '.format(value) + operand
            elif field == 'doi':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value.upper() + '%'
                    value = "'{}'".format(value)
                    if network_enabled:
                        value = value.replace("'", "\\'")
                    logger.info('DOI: ' + value)
                    interface_query += ' doi iLIKE {} '.format(value) + operand
            elif field == 'conference_display_name':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value.upper() + '%'
                    value = "'{}'".format(value)
                    if network_enabled:
                        value = value.replace("'", "\\'")
                    logger.info('Conference Display Name: ' + value)
                    interface_query += ' conference_display_name iLIKE {} '.format(value) + operand
            elif field == 'paper_title':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value + '%'
                    value = "'{}'".format(value)
                    if network_enabled:
                        value = value.replace("'", "\\'")
                    logger.info("Title: " + value)
                    interface_query += ' paper_title_tsv @@ to_tsquery ({}) '.format(value) + operand
                    # authors.append(value)
            elif field == 'paper_abstract':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value + '%'
                    value = "'{}'".format(value)
                    if network_enabled:
                        value = value.replace("'", "\\'")
                    logger.info("Paper Abstract: " + value)
                    interface_query += ' paper_abstract_tsv @@ to_tsquery ({}) '.format(value) + operand

    if network_enabled:
        interface_query = interface_query + 'LIMIT' + ' ' + '1000'
    else:
        interface_query = interface_query + 'LIMIT' + ' ' + '10000'
    logger.info("Query: " + interface_query)
    return interface_query


def convert_csv_to_json(csv_path, json_path, output_filter_string):
    csvfile = open(csv_path, 'r')
    jsonfile = open(json_path, 'w')

    fieldnames = tuple(output_filter_string.split(','))
    reader = csv.DictReader(csvfile, fieldnames)
    for row in reader:
        json.dump(row, jsonfile)
        jsonfile.write('\n')


def degree_0_query(interface_query, csv_file_name):
    neo4j_query = "CALL apoc.export.csv.query('CALL apoc.load.jdbc(\\'postgresql_url\\'," \
                  " \"" + interface_query + \
                  "\") YIELD row RETURN row.paper_id AS paper_id, " \
                  "row.author_id AS author_id," \
                  "row.author_sequence_number AS author_sequence_number," \
                  "row.authors_display_name AS authors_display_name," \
                  "row.authors_last_known_affiliation_id AS authors_last_known_affiliation_id," \
                  "row.journal AS journal_id," \
                  "row.conference_series_id AS conference_series_id," \
                  "row.conference_instance_id AS conference_instance_id," \
                  "row.paper_reference_id AS paper_reference_id," \
                  "row.field_of_study_id AS field_of_study_id," \
                  "row.doi AS doi," \
                  "row.doc_type AS doc_type," \
                  "row.paper_title AS paper_title," \
                  "row.original_title AS original_title," \
                  "row.book_title AS book_title," \
                  "row.year AS year," \
                  "row.date AS date," \
                  "row.paper_publisher AS paper_publisher," \
                  "row.issue AS issue," \
                  "row.paper_abstract AS paper_abstract," \
                  "row.paper_first_page AS paper_first_page," \
                  "row.paper_last_page AS paper_last_page," \
                  "row.paper_reference_count AS paper_reference_count," \
                  "row.paper_citation_count AS paper_citation_count," \
                  "row.paper_estimated_citation AS paper_estimated_citation," \
                  "row.conference_display_name AS conference_display_name," \
                  "row.journal_display_name AS journal_display_name," \
                  "row.journal_issn AS journal_issn," \
                  "row.journal_publisher AS journal_publisher', '" + csv_file_name + "',"\
                  "{format: 'plain'})"
    logger.info(neo4j_query)
    return neo4j_query


def get_edge_list_degree_1(csv_file_name, edge_file_name):
    csv_file_name = "file:///" + csv_file_name
    logger.info(csv_file_name)
    neo4j_query = "CALL apoc.export.csv.query('LOAD CSV WITH HEADERS FROM \\'" + csv_file_name + "\\'" \
                  " AS pg_pap MATCH(n:paper{paper_id:pg_pap.`paper_id`}) <- [:REFERENCES]-(m:paper) " \
                  "RETURN n.paper_id AS From , m.paper_id AS To','" + edge_file_name + "', {})"
    logger.info(neo4j_query)
    return neo4j_query


def get_edge_list_degree_2(csv_file_name, edge_file_name):
    csv_file_name = "file:///" + csv_file_name
    logger.info(csv_file_name)
    neo4j_query = "CALL apoc.export.csv.query('LOAD CSV WITH HEADERS FROM \\'" + csv_file_name + "\\'" \
                  " AS pg_pap MATCH(n:paper{paper_id:pg_pap.`paper_id`}) <- [:REFERENCES]-(m:paper) " \
                  "RETURN n.paper_id AS From , m.paper_id AS To UNION ALL " \
                  "LOAD CSV WITH HEADERS FROM \\'" + csv_file_name + "\\' as pg_pap " \
                  "MATCH (n:paper{paper_id:pg_pap.`paper_id`})<-[:REFERENCES]-(m:paper)<-[:REFERENCES]-(o:paper) "\
                  "RETURN m.paper_id AS From, o.paper_id AS To','" + edge_file_name + "',{})"
    logger.info(neo4j_query)
    return neo4j_query


def get_node_list(edge_file_name, node_file_name):
    edge_file_name = "file:///" + edge_file_name
    logger.info(edge_file_name)
    neo4j_query = "CALL apoc.export.csv.query('LOAD CSV WITH HEADERS FROM \\'" + edge_file_name + "\\' " \
                  "as edge MATCH(n:paper) WHERE n.paper_id IN [edge.`From`, edge.`To`] " \
                  "RETURN DISTINCT(n.paper_id) AS paper_id," \
                  "n.date AS date,"\
                  "n.journal_id AS journal_id,"\
                  "n.citation_count AS citation_count,"\
                  "n.original_title AS original_title,"\
                  "n.issue AS issue,"\
                  "n.paper_title AS paper_title,"\
                  "n.year AS year,"\
                  "n.first_name AS first_name,"\
                  "n.last_name AS last_name,"\
                  "n.original_venue AS original_venue,"\
                  "n.doc_type AS doc_type,"\
                  "n.volume AS volume,"\
                  "n.estimated_citation AS estimated_citation,"\
                  "n.conference_instance_id AS conference_instance_id,"\
                  "n.book_title AS book_title,"\
                  "n.rank AS rank,"\
                  "n.publisher AS publisher,"\
                  "n.created_date AS created_date,"\
                  "n.reference_count AS reference_count,"\
                  "n.conference_series_id AS conference_series_id,"\
                  "n.doi AS doi','" + node_file_name + "', {})"
    logger.info(neo4j_query)
    return neo4j_query


def degree_1_query(interface_query, node_file_name, edge_file_name):
    neo4j_query = "CALL apoc.load.jdbc('postgresql_url'," \
                  " '" + interface_query + \
                  "') YIELD row MATCH (n:paper)<-[r:REFERENCES]-(m:paper)" \
                  " WHERE n.paper_id = row.paper_id WITH collect(distinct m) + n as nodes, " \
                  "collect(distinct r) as relationships CALL apoc.export.csv.data([], relationships, '" + edge_file_name + "', {}) " \
                  "YIELD file as edgefile CALL apoc.export.csv.data(nodes, [], '" + node_file_name + "', {}) " \
                  "YIELD file as nodefile RETURN nodes, relationships"
    return neo4j_query


def degree_2_query(interface_query, node_file_name, edge_file_name):
    neo4j_query = "CALL apoc.load.jdbc('postgresql_url'," \
                  " '" + interface_query + \
                  "') YIELD row MATCH (n:paper)<-[r:REFERENCES]-(m:paper)<-[s:REFERENCES]-(o:paper)" \
                  " WHERE n.paper_id = row.paper_id WITH collect(distinct m)  + collect(distinct o) + n as nodes, " \
                  "collect(distinct r) + collect(distinct s) as relationships CALL apoc.export.csv.data(nodes, [],  '" + node_file_name + "', {}) " \
                  "YIELD file as nodefile CALL apoc.export.csv.data([], relationships, '" + edge_file_name + "', {}) " \
                  "YIELD file as edgefile RETURN nodes, relationships"
    return neo4j_query


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
            driver_session = mag_driver.session()
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
                    logger.info(network_query_type)
                    updateStatement = "UPDATE user_job SET job_status = 'RUNNING', modified_on = CURRENT_TIMESTAMP WHERE job_id = (%s)"
                    # Execute the SQL Query
                    meta_db_cursor.execute(updateStatement, (job_id,))
                    meta_connection.commit()
                    s3_client = boto3.resource('s3',
                                               aws_access_key_id=util.config_reader.get_aws_access_key(),
                                               aws_secret_access_key=util.config_reader.get_aws_access_key_secret(),
                                               region_name=util.config_reader.get_aws_region())
                    root_bucket_name = 'cadre-query-result'
                    bucket_location = username + '/query-results/'

                    # Generating the Query that needs to run on the RDS
                    try:
                        efs_root = util.config_reader.get_cadre_efs_root_query_results_listener()
                        neo4j_import_datasets = util.config_reader.get_mag_graph_db_import_dir()
                        neo4j_import_listener = util.config_reader.get_cadre_efs_root_neo4j_output_listener()
                        user_query_result_dir = efs_root + '/' + username + '/query-results'
                        if not os.path.exists(user_query_result_dir):
                            os.makedirs(user_query_result_dir)
                        # shutil.chown(user_query_result_dir, user='ubuntu', group='ubuntu')
                        csv_path = user_query_result_dir + '/' + job_id + '.csv'
                        logger.info(csv_path)
                        csv_name = job_id + '.csv'
                        node_path = job_id + '_nodes.csv'
                        edge_path = job_id + '_edges.csv'
                        logger.info(node_path)
                        logger.info(edge_path)
                        if dataset == 'wos':
                            logger.info('User selects WOS dataset !!!')
                            if network_query_type == 'citations':
                                # output_filters_single.append('paper_reference_id')
                                # output_filter_string = ",".join(output_filters_single)
                                # interface_query = generate_wos_query(output_filter_string, filters)
                                logger.info("Not yet supported...")
                            else:
                                network_enabled = False
                                interface_query = generate_wos_query(output_filter_string, filters, network_enabled)
                                logger.info(interface_query)
                                output_query = "COPY ({}) TO STDOUT WITH CSV HEADER".format(interface_query)
                                with open(csv_path, 'w') as f:
                                    wos_cursor.copy_expert(output_query, f)
                                s3_client.meta.client.upload_file(csv_path, root_bucket_name,
                                                                  bucket_location + job_id + '.csv')
                        else:
                            logger.info('User selects MAG dataset !!!')
                            if network_query_type == 'citations':
                                logger.info('citations')
                                network_enabled = True
                                output_filters_single.append('paper_id')
                                output_filter_string = ",".join(output_filters_single)
                                logger.info(output_filter_string)
                                interface_query = generate_mag_query(output_filter_string, filters, network_enabled)
                                logger.info(interface_query)
                                if degree == 1:
                                    degree_0_q = degree_0_query(interface_query, csv_name)
                                    edge_query = get_edge_list_degree_1(csv_name, edge_path)
                                    node_query = get_node_list(edge_path, node_path)
                                    degree_0_results = driver_session.run(degree_0_q)
                                    edge_result = driver_session.run(edge_query)
                                    node_result = driver_session.run(node_query)
                                    entire_result_degree_0 = []  # Will contain all the items
                                    edge_result_degree_1 = []  # Will contain all the items
                                    node_result_degree_1 = []  # Will contain all the items
                                    for record in entire_result_degree_0:
                                        entire_result_degree_0.append(record)
                                    for record in edge_result_degree_1:
                                        edge_result_degree_1.append(record)
                                    for record in node_result_degree_1:
                                        node_result_degree_1.append(record)
                                elif degree == 2:
                                    degree_0_q = degree_0_query(interface_query, csv_name)
                                    logger.info(degree_0_q)
                                    edge_query = get_edge_list_degree_2(csv_name, edge_path)
                                    logger.info(edge_query)
                                    node_query = get_node_list(edge_path, node_path)
                                    logger.info(node_query)
                                    degree_0_results = driver_session.run(degree_0_q)
                                    edge_result = driver_session.run(edge_query)
                                    node_result = driver_session.run(node_query)
                                    entire_result_degree_0 = []  # Will contain all the items
                                    edge_result_degree_1 = []  # Will contain all the items
                                    node_result_degree_1 = []  # Will contain all the items
                                    for record in entire_result_degree_0:
                                        entire_result_degree_0.append(record)
                                    for record in edge_result_degree_1:
                                        edge_result_degree_1.append(record)
                                    for record in node_result_degree_1:
                                        node_result_degree_1.append(record)
                                else:
                                    logger.info("Degree 1 and 2 are supported. If degree is more than that, it will use 2 as default. ")
                                    degree_0_q = degree_0_query(interface_query, csv_name)
                                    edge_query = get_edge_list_degree_2(csv_name, edge_path)
                                    node_query = get_node_list(edge_path, node_path)
                                    degree_0_results = driver_session.run(degree_0_q)
                                    edge_result = driver_session.run(edge_query)
                                    node_result = driver_session.run(node_query)
                                    entire_result_degree_0 = []  # Will contain all the items
                                    edge_result_degree_1 = []  # Will contain all the items
                                    node_result_degree_1 = []  # Will contain all the items
                                    for record in entire_result_degree_0:
                                        entire_result_degree_0.append(record)
                                    for record in edge_result_degree_1:
                                        edge_result_degree_1.append(record)
                                    for record in node_result_degree_1:
                                        node_result_degree_1.append(record)
                                # copy files to correct EFS location and s3 locations
                                source_csv_path = neo4j_import_listener + '/' + csv_name
                                target_csv_path = user_query_result_dir + '/' + csv_name
                                logger.info(source_csv_path)
                                logger.info(target_csv_path)
                                copyfile(source_csv_path, target_csv_path)

                                source_node_path = neo4j_import_listener + '/' + node_path
                                target_node_path = user_query_result_dir + '/' + node_path
                                logger.info(source_node_path)
                                logger.info(target_node_path)
                                copyfile(source_node_path, target_node_path)

                                source_edge_path = neo4j_import_listener + '/' + edge_path
                                target_edge_path = user_query_result_dir + '/' + edge_path
                                logger.info(source_edge_path)
                                logger.info(target_edge_path)
                                copyfile(source_edge_path, target_edge_path)
                                driver_session.commit()
                            else:
                                network_enabled = False
                                interface_query = generate_mag_query(output_filter_string, filters, network_enabled)
                                logger.info(interface_query)
                                output_query = "COPY ({}) TO STDOUT WITH CSV HEADER".format(interface_query)
                                with open(csv_path, 'w') as f:
                                    mag_cursor.copy_expert(output_query, f)
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
                    driver_session.close()
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
