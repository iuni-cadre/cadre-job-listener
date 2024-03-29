import csv
import json
import logging.config
import os
import sys
import traceback

import boto3
import psycopg2 as psycopg2
from shutil import copyfile

abspath = os.path.abspath(os.path.dirname(__file__))
cadre = os.path.dirname(abspath)
util = cadre + '/util'
conf = cadre + '/conf'
sys.path.append(cadre)

import util.config_reader
import util.tool_util
from util.db_util import cadre_meta_connection_pool

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

# If applicable, delete the existing log file to generate a fresh log file during each execution
# logfile_path = cadre + "/cadre_job_listener.log"
# if path.isfile(logfile_path):
#     os.remove(logfile_path)

output_fileters_map_mag_graph = {
    "author_sequence_number": "author_sequence_number::varchar",
    "date": "date::varchar",
    "paper_reference_count": "paper_reference_count::varchar",
    "paper_citation_count": "paper_citation_count::varchar",
    "paper_estimated_citation": "paper_estimated_citation::varchar"
}

output_fileters_map_wos_graph = {
    "year": "year::varchar",
    "number": "number::varchar",
    "issue": "issue::varchar",
    "pages": "pages::varchar",
    "authors_full_name": "authors_full_name::varchar",
    "authors_id_orcid": "authors_id_orcid::varchar",
    "authors_id_dais": "authors_id_dais::varchar",
    "authors_id_research": "authors_id_research::varchar",
    "authors_prefix": "authors_prefix::varchar",
    "authors_first_name": "authors_first_name::varchar",
    "authors_middle_name": "authors_middle_name::varchar",
    "authors_last_name": "authors_last_name::varchar",
    "authors_suffix": "authors_suffix::varchar",
    "authors_initials": "authors_initials::varchar",
    "authors_display_name": "authors_display_name::varchar",
    "authors_wos_name": "authors_wos_name::varchar",
    "authors_id_lang": "authors_id_lang::varchar",
    "authors_email": "authors_email::varchar",
    "reference": "reference::varchar",
    "issn": "issn::varchar",
    "doi": "doi::varchar",
    "title": "title::varchar",
    "journal_name": "journals_name::varchar",
    "journal_abbrev": "journals_abbrev::varchar",
    "journal_iso": "journals_iso::varchar",
    "abstract_paragraph": "abstract_paragraphs::varchar"
}

degree_0_fields_map_mag_graph = {
    "paper_id": "row.paper_id AS paper_id,",
    "author_id": "row.author_id AS author_id,",
    "author_sequence_number::varchar":"row.author_sequence_number AS author_sequence_number,",
    "authors_display_name":"row.authors_display_name AS authors_display_name,",
    "authors_last_known_affiliation_id":"row.authors_last_known_affiliation_id AS authors_last_known_affiliation_id,",
    "journal_id": "row.journal AS journal_id,",
    "conference_series_id": "row.conference_series_id AS conference_series_id,",
    "conference_instance_id": "row.conference_instance_id AS conference_instance_id,",
    "paper_reference_id": "row.paper_reference_id AS paper_reference_id,",
    "field_of_study_id": "row.field_of_study_id AS field_of_study_id,",
    "doi": "row.doi AS doi,",
    "doc_type": "row.doc_type AS doc_type,",
    "paper_title": "row.paper_title AS paper_title,",
    "original_title": "row.original_title AS original_title,",
    "book_title": "row.book_title AS book_title,",
    "year": "row.year AS year,",
    "date::varchar": "row.date AS date,",
    "paper_publisher": "row.paper_publisher AS paper_publisher,",
    "issue": "row.issue AS issue,",
    "paper_abstract": "row.paper_abstract AS paper_abstract,",
    "paper_first_page": "row.paper_first_page AS paper_first_page,",
    "paper_last_page": "row.paper_last_page AS paper_last_page,",
    "paper_reference_count::varchar": "row.paper_reference_count AS paper_reference,",
    "paper_citation_count::varchar": "row.paper_citation_count AS paper_citation_count,",
    "paper_estimated_citation::varchar": "row.paper_estimated_citation AS paper_estimated_citation,",
    "conference_display_name": "row.conference_display_name AS conference_display_name,",
    "journal_display_name": "row.journal_display_name AS journal_display_name,",
    "journal_issn": "row.journal_issn AS journal_issn,",
    "journal_publisher": "row.journal_publisher AS journal_publisher,",
}

degree_0_fields_map_wos_graph = {
    "id": "row.id as wos_id,",
    "year": "row.year as year,",
    "number": "row.number as number,",
    "issue": "row.issue as issue, ",
    "pages": "row.pages as pages,",
    "authors_full_name": "row.authors_full_name as authors_full_name,",
    "authors_id_orcid": "row.authors_id_orcid as authors_id_orcid,",
    "authors_id_dais": "row.authors_id_dais as authors_id_dais,",
    "authors_id_research": "row.authors_id_research as authors_id_research,",
    "authors_prefix": "row.authors_prefix as authors_prefix,",
    "authors_first_name": "row.authors_first_name as authors_first_name,",
    "authors_middle_name": "row.authors_middle_name as authors_middle_name,",
    "authors_last_name": "row.authors_last_name as authors_last_name,",
    "authors_suffix": "row.authors_suffix as authors_suffix,",
    "authors_initials": "row.authors_initials as authors_initials,",
    "authors_display_name": "row.authors_display_name as authors_display_name,",
    "authors_wos_name": "row.authors_wos_name as authors_wos_name,",
    "authors_id_lang": "row.authors_id_lang as authors_id_lang,",
    "authors_email": "row.authors_email as authors_email,",
    "reference": "row.reference as reference,",
    "issn": "row.issn as issn,",
    "doi": "row.doi as doi,",
    "title": "row.title as title,",
    "journal_name": "row.journals_name as journal_name,",
    "journal_abbrev": "row.journals_abbrev as journal_abbrev,",
    "journal_iso": "row.journals_iso as journal_iso,",
    "abstract_paragraph": "row.abstract_paragraphs as abstract_paragraph,",
    "reference_count": "row.reference_count AS reference_count,"
}


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
            elif field == 'journal_name':
                if value is not None:
                    value = value.strip()
                    value = value.replace(' ', '%')
                    value = '%' + value + '%'
                    value = "'{}'".format(value)
                    if network_enabled:
                        value = value.replace("'", "\\'")
                    logger.info("Journal Name: " + value)
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
        interface_query = interface_query + 'LIMIT' + ' ' + '100000'
    else:
        interface_query = interface_query + 'LIMIT' + ' ' + '100000'
    print("Query: " + interface_query)
    return interface_query


def generate_mag_query(output_filter_string, query_json, network_enabled):
    logger.info(output_filter_string)
    logger.info(query_json)
    # if network_enabled:
    #     output_filter_string = "paper_id, author_id, author_sequence_number::varchar," \
    #               "authors_display_name,authors_last_known_affiliation_id,journal_id,conference_series_id,conference_instance_id,"\
    #               "paper_reference_id,field_of_study_id,doi,doc_type,paper_title,original_title,book_title,year,date::varchar,"\
    #               "paper_publisher,issue,paper_abstract,paper_first_page,paper_last_page,paper_reference_count::varchar,"\
    #               "paper_citation_count::varchar,paper_estimated_citation::varchar,conference_display_name,journal_display_name,"\
    #               "journal_issn,journal_publisher"
    interface_query = 'SELECT ' + output_filter_string + ' FROM mag_core.interface_table WHERE '
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
        interface_query = interface_query + 'LIMIT' + ' ' + '100000'
    else:
        interface_query = interface_query + 'LIMIT' + ' ' + '100000'
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


def degree_0_query(interface_query, csv_file_name, csv_field_names):
    neo4j_query = "CALL apoc.export.csv.query('CALL apoc.load.jdbc(\\'postgresql_url\\'," \
                  " \"" + interface_query + \
                  "\") YIELD row RETURN " + csv_field_names + "', '" + csv_file_name + "',"\
                  "{format: 'plain'})"
    logger.info(neo4j_query)
    return neo4j_query


def get_edge_list_degree_1_mag(csv_file_name, edge_file_name):
    csv_file_name = "file:///" + csv_file_name
    logger.info(csv_file_name)
    neo4j_query = "CALL apoc.export.csv.query('LOAD CSV WITH HEADERS FROM \\'" + csv_file_name + "\\'" \
                  " AS pg_pap MATCH(n:paper{paper_id:pg_pap.`paper_id`}) <- [:REFERENCES]-(m:paper) " \
                  "RETURN n.paper_id AS Citing , m.paper_id AS Cited','" + edge_file_name + "', {})"
    logger.info(neo4j_query)
    return neo4j_query


def get_edge_list_degree_2_mag(csv_file_name, edge_file_name):
    csv_file_name = "file:///" + csv_file_name

    neo4j_query = "CALL apoc.export.csv.query('LOAD CSV WITH HEADERS FROM \\'" + csv_file_name + "\\'" \
                  " AS pg_pap MATCH (n:paper{paper_id:pg_pap.`paper_id`}) <- [:REFERENCES]-(m:paper) " \
                  "WITH COLLECT ({from:n.paper_id, to: m.paper_id}) AS data1" + "," \
                  " [(m)<-[:REFERENCES]-(o:paper) | {from: m.paper_id, to: o.paper_id}] AS data2" +  \
                  "  UNWIND (data1 + data2) AS data RETURN data.from AS Citing, data.to AS Cited','" + edge_file_name + "',{})"
    logger.info(neo4j_query)
    return neo4j_query


def get_node_list_mag(edge_file_name, node_file_name):
    edge_file_name = "file:///" + edge_file_name
    logger.info(edge_file_name)
    neo4j_query = "CALL apoc.export.csv.query('LOAD CSV WITH HEADERS FROM \\'" + edge_file_name + "\\' " \
                  "as edge MATCH(n:paper) WHERE n.paper_id IN [edge.`Citing`, edge.`Cited`] " \
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


def get_edge_list_degree_1_wos(csv_file_name, edge_file_name):
    csv_file_name = "file:///" + csv_file_name
    logger.info(csv_file_name)
    neo4j_query = "CALL apoc.export.csv.query('LOAD CSV WITH HEADERS FROM \\'" + csv_file_name + "\\'" \
                  " AS pg_pap MATCH(n:paper{paper_id:pg_pap.`wos_id`}) <- [:REFERENCES]-(m:paper) " \
                  "RETURN n.paper_id AS Citing , m.paper_id AS Cited','" + edge_file_name + "', {})"
    logger.info(neo4j_query)
    return neo4j_query


def get_edge_list_degree_2_wos(csv_file_name, edge_file_name):
    csv_file_name = "file:///" + csv_file_name
    neo4j_query = "CALL apoc.export.csv.query('LOAD CSV WITH HEADERS FROM \\'" + csv_file_name + "\\'" \
                  " AS pg_pap MATCH (n:paper{paper_id:pg_pap.`wos_id`}) <- [:REFERENCES]-(m:paper) " \
                  "WITH COLLECT ({from:n.paper_id, to: m.paper_id}) AS data1" + "," \
                  " [(m)<-[:REFERENCES]-(o:paper) | {from: m.paper_id, to: o.paper_id}] AS data2" +  \
                  "  UNWIND (data1 + data2) AS data RETURN data.from AS Citing, data.to AS Cited','" + edge_file_name + "',{})"
    logger.info(neo4j_query)
    return neo4j_query


def get_node_list_wos(edge_file_name, node_file_name):
    edge_file_name = "file:///" + edge_file_name
    logger.info(edge_file_name)
    neo4j_query = "CALL apoc.export.csv.query('LOAD CSV WITH HEADERS FROM \\'" + edge_file_name + "\\' " \
                  "as edge MATCH(n:paper) WHERE n.paper_id IN [edge.`Citing`, edge.`Cited`] " \
                  "RETURN DISTINCT(n.paper_id) AS paper_id," \
                  "n.pubyear AS pubyear,"\
                  "n.issue AS issue,"\
                  "n.issn AS issn,"\
                  "n.has_abstract AS has_abstract,"\
                  "n.authors_full_name AS authors_full_name,"\
                  "n.journal_name AS journal_name,"\
                  "n.pubtype AS pubtype,"\
                  "n.title AS title,"\
                  "n.vol AS vol','" + node_file_name + "', {})"
    logger.info(neo4j_query)
    return neo4j_query


def generate_output_string_neo4j_mag(output_filters):
    logger.info(output_filters)
    for output in output_filters:
        logger.info(output)
        if output in output_fileters_map_mag_graph:
            output_filters.remove(output)
            output_filters.append(output_fileters_map_mag_graph[output])
    output_string = ",".join(output_filters)
    logger.info(output_string)
    return output_string


def generate_csv_fields_neo4j_mag(output_filters):
    output_string = ''
    for output in output_filters:
        output_string += degree_0_fields_map_mag_graph[output]
    output_string = output_string[:-1]
    logger.info(output_string)
    return output_string


def generate_output_string_neo4j_wos(output_filters):
    logger.info(output_filters)
    graph_output_fields = []
    for output in output_filters:
        logger.info(output)
        if output in output_fileters_map_wos_graph:
            graph_output_fields.append(output_fileters_map_wos_graph[output])
        else:
            graph_output_fields.append(output)
    output_string = ",".join(graph_output_fields)
    logger.info(output_string)
    return output_string


def generate_csv_fields_neo4j_wos(output_filters):
    output_string = ''
    for output in output_filters:
        if output in degree_0_fields_map_wos_graph.keys():
            output_string += degree_0_fields_map_wos_graph[output]
    output_string = output_string[:-1]
    logger.info(output_string)
    return output_string


def get_file_name(job_id, job_name):
    logger.info(job_name)
    if job_id == job_name or job_name == '':
        file_name = job_id
    else:
        file_name = job_name + '_' + job_id
    return file_name


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
            mag_driver_session = mag_driver.session()
            wos_driver_session = wos_driver.session()
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
                    job_name = query_json['job_name']
                    username = query_json['username']
                    user_id = query_json['user_id']
                    output_fields = query_json['output']

                    # Delete received message from queue
                    sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )

                    for output_filed in output_fields:
                        type = output_filed['type']
                        if type == 'single':
                            field = output_filed['field']
                            if field == 'wos_id':
                                output_filters_single.append('id')
                            elif field == 'references':
                                output_filters_single.append("\\'references\\'")
                            else:
                                output_filters_single.append(field)
                        else:
                            network_query_type = output_filed['field']
                            degree = int(output_filed['degree'])
                    output_filter_string = ",".join(output_filters_single)
                    logger.info(output_filter_string)
                    # Updating the job status in the job database as running
                    logger.info(network_query_type)
                    job_update_statement = "UPDATE user_job SET job_status = 'RUNNING', modified_on = CURRENT_TIMESTAMP WHERE job_id = (%s)"
                    # Execute the SQL Query
                    meta_db_cursor.execute(job_update_statement, (job_id,))
                    meta_connection.commit()
                    try:
                        efs_root = util.config_reader.get_cadre_efs_root_query_results_listener()
                        efs_subpath = util.config_reader.get_cadre_efs_subpath_query_results_listener()
                        efs_path = efs_root + efs_subpath
                        neo4j_mag_import_efs_dir = util.config_reader.get_cadre_efs_root_neo4j_mag_output_listener()
                        neo4j_wos_import_efs_dir = util.config_reader.get_cadre_efs_root_neo4j_wos_output_listener()
                        user_query_result_dir = efs_path + '/' + username + '/query-results'
                        if not os.path.exists(user_query_result_dir):
                            os.makedirs(user_query_result_dir)
                        # shutil.chown(user_query_result_dir, user='ubuntu', group='ubuntu')
                        file_name = get_file_name(job_id, job_name)
                        csv_path = user_query_result_dir + '/' + file_name + '.csv'
                        logger.info(csv_path)
                        csv_name = file_name + '.csv'
                        node_path = file_name + '_nodes.csv'
                        edge_path = file_name + '_edges.csv'
                        logger.info(node_path)
                        logger.info(edge_path)
                        file_insert_statement = "INSERT INTO query_result" \
                                                "(job_id,efs_path, file_checksum, data_type, authenticity, created_by, created_on) " \
                                                "VALUES(%s,%s,%s,%s,%s,%s,current_timestamp )"
                        if dataset == 'wos':
                            logger.info('User selects WOS dataset !!!')
                            if network_query_type == 'references':
                                logger.info(network_query_type)
                                network_enabled = True
                                if 'id' not in output_filters_single:
                                    output_filters_single.append('id')
                                # generate output filter string for neo4j
                                output_filter_string = generate_output_string_neo4j_wos(output_filters_single)
                                logger.info(output_filter_string)
                                interface_query = generate_wos_query(output_filter_string, filters, network_enabled)
                                logger.info(interface_query)
                                degree_0_field_names = generate_csv_fields_neo4j_wos(output_filters_single)
                                if degree == 1:
                                    degree_0_q = degree_0_query(interface_query, csv_name, degree_0_field_names)
                                    edge_query = get_edge_list_degree_1_wos(csv_name, edge_path)
                                    node_query = get_node_list_wos(edge_path, node_path)
                                    degree_0_results = wos_driver_session.run(degree_0_q)
                                    edge_result = wos_driver_session.run(edge_query)
                                    node_result = wos_driver_session.run(node_query)
                                    entire_result_degree_0 = []  # Will contain all the items
                                    edge_result_degree_1 = []  # Will contain all the items
                                    node_result_degree_1 = []  # Will contain all the items
                                    for record in degree_0_results:
                                        entire_result_degree_0.append(record)
                                    for record in edge_result:
                                        edge_result_degree_1.append(record)
                                    for record in node_result:
                                        node_result_degree_1.append(record)
                                elif degree == 2:
                                    degree_0_q = degree_0_query(interface_query, csv_name, degree_0_field_names)
                                    logger.info(degree_0_q)
                                    edge_query = get_edge_list_degree_2_wos(csv_name, edge_path)
                                    logger.info(edge_query)
                                    node_query = get_node_list_wos(edge_path, node_path)
                                    logger.info(node_query)
                                    degree_0_results = wos_driver_session.run(degree_0_q)
                                    edge_result = wos_driver_session.run(edge_query)
                                    node_result = wos_driver_session.run(node_query)
                                    entire_result_degree_0 = []  # Will contain all the items
                                    edge_result_degree_1 = []  # Will contain all the items
                                    node_result_degree_1 = []  # Will contain all the items
                                    for record in degree_0_results:
                                        entire_result_degree_0.append(record)
                                    for record in edge_result:
                                        edge_result_degree_1.append(record)
                                    for record in node_result:
                                        node_result_degree_1.append(record)
                                else:
                                    logger.info(
                                        "Degree 1 and 2 are supported. If degree is more than that, it will use 2 as default. ")
                                    degree_0_q = degree_0_query(interface_query, csv_name, degree_0_field_names)
                                    edge_query = get_edge_list_degree_2_wos(csv_name, edge_path)
                                    node_query = get_node_list_wos(edge_path, node_path)
                                    degree_0_results = wos_driver_session.run(degree_0_q)
                                    edge_result = wos_driver_session.run(edge_query)
                                    node_result = wos_driver_session.run(node_query)

                                    entire_result_degree_0 = []  # Will contain all the items
                                    edge_result_degree_1 = []  # Will contain all the items
                                    node_result_degree_1 = []  # Will contain all the items

                                    for record in degree_0_results:
                                        entire_result_degree_0.append(record)
                                    for record in edge_result:
                                        edge_result_degree_1.append(record)
                                    for record in node_result:
                                        node_result_degree_1.append(record)
                                # copy files to correct EFS location
                                source_csv_path = neo4j_wos_import_efs_dir + '/' + csv_name
                                target_csv_path = user_query_result_dir + '/' + csv_name
                                logger.info(source_csv_path)
                                logger.info(target_csv_path)
                                copyfile(source_csv_path, target_csv_path)

                                source_node_path = neo4j_wos_import_efs_dir + '/' + node_path
                                target_node_path = user_query_result_dir + '/' + node_path
                                logger.info(source_node_path)
                                logger.info(target_node_path)
                                copyfile(source_node_path, target_node_path)

                                source_edge_path = neo4j_wos_import_efs_dir + '/' + edge_path
                                target_edge_path = user_query_result_dir + '/' + edge_path
                                logger.info(source_edge_path)
                                logger.info(target_edge_path)
                                copyfile(source_edge_path, target_edge_path)

                                # get checksums and update the db
                                target_csv_checksum = util.tool_util.get_file_checksum(target_csv_path)
                                target_edge_file_checksum = util.tool_util.get_file_checksum(target_edge_path)
                                target_node_file_checksum = util.tool_util.get_file_checksum(target_node_path)

                                csv_file_insert_data = (job_id, target_csv_path, target_csv_checksum, 'WOS', 'TRUE', user_id)
                                edge_file_insert_data = (job_id, target_edge_path, target_edge_file_checksum, 'WOS', 'TRUE', user_id)
                                node_file_insert_data = (job_id, target_node_path, target_node_file_checksum, 'WOS', 'TRUE', user_id)
                                # Execute the SQL Query
                                meta_db_cursor.execute(file_insert_statement, csv_file_insert_data)
                                meta_connection.commit()
                                meta_db_cursor.execute(file_insert_statement, edge_file_insert_data)
                                meta_connection.commit()
                                meta_db_cursor.execute(file_insert_statement, node_file_insert_data)
                                meta_connection.commit()
                            else:
                                network_enabled = False
                                interface_query = generate_wos_query(output_filter_string, filters, network_enabled)
                                logger.info(interface_query)
                                output_query = "COPY ({}) TO STDOUT WITH CSV HEADER".format(interface_query)
                                with open(csv_path, 'w') as f:
                                    wos_cursor.copy_expert(output_query, f)
                                target_csv_checksum = util.tool_util.get_file_checksum(csv_path)

                                csv_file_insert_data = (job_id, csv_path, target_csv_checksum, 'WOS', 'TRUE', user_id)
                                meta_db_cursor.execute(file_insert_statement, csv_file_insert_data)
                                meta_connection.commit()

                        else:
                            logger.info('User selects MAG dataset !!!')
                            if network_query_type == 'citations':
                                logger.info('citations')
                                network_enabled = True
                                if 'paper_id' not in output_filters_single:
                                    output_filters_single.append('paper_id')
                                # generate output filter string for neo4j
                                logger.info(output_filters_single)
                                output_filter_string = generate_output_string_neo4j_mag(output_filters_single)
                                logger.info(output_filter_string)
                                interface_query = generate_mag_query(output_filter_string, filters, network_enabled)
                                logger.info(interface_query)
                                degree_0_field_names = generate_csv_fields_neo4j_mag(output_filters_single)
                                if degree == 1:
                                    degree_0_q = degree_0_query(interface_query, csv_name, degree_0_field_names)
                                    logger.info(degree_0_q)
                                    edge_query = get_edge_list_degree_1_mag(csv_name, edge_path)
                                    node_query = get_node_list_mag(edge_path, node_path)
                                    degree_0_results = mag_driver_session.run(degree_0_q)
                                    logger.info('Degree 0 query executed...')
                                    edge_result = mag_driver_session.run(edge_query)
                                    logger.info('Edge query executed...')
                                    node_result = mag_driver_session.run(node_query)
                                    logger.info('Node query executed...')
                                    entire_result_degree_0 = []  # Will contain all the items
                                    edge_result_degree_1 = []  # Will contain all the items
                                    node_result_degree_1 = []  # Will contain all the items

                                    for record in degree_0_results:
                                        entire_result_degree_0.append(record)
                                    for record in edge_result:
                                        edge_result_degree_1.append(record)
                                    for record in node_result:
                                        node_result_degree_1.append(record)
                                elif degree == 2:
                                    degree_0_q = degree_0_query(interface_query, csv_name, degree_0_field_names)
                                    logger.info(degree_0_q)
                                    edge_query = get_edge_list_degree_2_mag(csv_name, edge_path)
                                    logger.info(edge_query)
                                    node_query = get_node_list_mag(edge_path, node_path)
                                    logger.info(node_query)
                                    degree_0_results = mag_driver_session.run(degree_0_q)
                                    edge_result = mag_driver_session.run(edge_query)
                                    node_result = mag_driver_session.run(node_query)
                                    entire_result_degree_0 = []  # Will contain all the items
                                    edge_result_degree_1 = []  # Will contain all the items
                                    node_result_degree_1 = []  # Will contain all the items
                                    for record in degree_0_results:
                                        entire_result_degree_0.append(record)
                                    for record in edge_result:
                                        edge_result_degree_1.append(record)
                                    for record in node_result:
                                        node_result_degree_1.append(record)
                                else:
                                    logger.info("Degree 1 and 2 are supported. If degree is more than that, it will use 2 as default. ")
                                    degree_0_q = degree_0_query(interface_query, csv_name, degree_0_field_names)
                                    edge_query = get_edge_list_degree_2_mag(csv_name, edge_path)
                                    node_query = get_node_list_mag(edge_path, node_path)
                                    degree_0_results = mag_driver_session.run(degree_0_q)
                                    edge_result = mag_driver_session.run(edge_query)
                                    node_result = mag_driver_session.run(node_query)

                                    entire_result_degree_0 = []  # Will contain all the items
                                    edge_result_degree_1 = []  # Will contain all the items
                                    node_result_degree_1 = []  # Will contain all the items

                                    for record in degree_0_results:
                                        entire_result_degree_0.append(record)
                                    for record in edge_result:
                                        edge_result_degree_1.append(record)
                                    for record in node_result:
                                        node_result_degree_1.append(record)

                                # copy files to correct EFS location
                                source_csv_path = neo4j_mag_import_efs_dir + '/' + csv_name
                                target_csv_path = user_query_result_dir + '/' + csv_name
                                logger.info(source_csv_path)
                                logger.info(target_csv_path)
                                copyfile(source_csv_path, target_csv_path)

                                source_node_path = neo4j_mag_import_efs_dir + '/' + node_path
                                target_node_path = user_query_result_dir + '/' + node_path
                                logger.info(source_node_path)
                                logger.info(target_node_path)
                                copyfile(source_node_path, target_node_path)

                                source_edge_path = neo4j_mag_import_efs_dir + '/' + edge_path
                                target_edge_path = user_query_result_dir + '/' + edge_path
                                logger.info(source_edge_path)
                                logger.info(target_edge_path)
                                copyfile(source_edge_path, target_edge_path)

                                # get checksums and update the db
                                target_csv_checksum = util.tool_util.get_file_checksum(target_csv_path)
                                target_edge_file_checksum = util.tool_util.get_file_checksum(target_edge_path)
                                target_node_file_checksum = util.tool_util.get_file_checksum(target_node_path)

                                csv_file_insert_data = (
                                job_id, target_csv_path, target_csv_checksum, 'MAG', 'TRUE', user_id)
                                edge_file_insert_data = (
                                job_id, target_edge_path, target_edge_file_checksum, 'MAG', 'TRUE', user_id)
                                node_file_insert_data = (
                                job_id, target_node_path, target_node_file_checksum, 'MAG', 'TRUE', user_id)
                                # Execute the SQL Query
                                meta_db_cursor.execute(file_insert_statement, csv_file_insert_data)
                                meta_connection.commit()
                                meta_db_cursor.execute(file_insert_statement, edge_file_insert_data)
                                meta_connection.commit()
                                meta_db_cursor.execute(file_insert_statement, node_file_insert_data)
                                meta_connection.commit()
                            else:
                                network_enabled = False
                                interface_query = generate_mag_query(output_filter_string, filters, network_enabled)
                                logger.info(interface_query)
                                output_query = "COPY ({}) TO STDOUT WITH CSV HEADER".format(interface_query)
                                with open(csv_path, 'w') as f:
                                    mag_cursor.copy_expert(output_query, f)
                                target_csv_checksum = util.tool_util.get_file_checksum(csv_path)

                                csv_file_insert_data = (job_id, csv_path, target_csv_checksum, 'MAG', 'TRUE', user_id)
                                meta_db_cursor.execute(file_insert_statement, csv_file_insert_data)
                                meta_connection.commit()
                    except (Exception) as error:
                        logger.exception(error)
                        logger.error("Error while executing graph query. Error is " + str(error))
                        logger.info("Job ID: " + job_id)
                        job_update_statement = "UPDATE user_job SET job_status = 'FAILED', modified_on = CURRENT_TIMESTAMP WHERE job_id = (%s)"
                        # Execute the SQL Query
                        meta_db_cursor.execute(job_update_statement, (job_id,))
                        meta_connection.commit()

                    print("Job ID: " + job_id)
                    job_update_statement = "UPDATE user_job SET job_status = 'COMPLETED', modified_on = CURRENT_TIMESTAMP WHERE job_id = (%s)"
                    # Execute the SQL Query
                    meta_db_cursor.execute(job_update_statement, (job_id,))
                    meta_connection.commit()
                except (Exception, psycopg2.Error) as error:
                    traceback.print_tb(error.__traceback__)
                    logger.error('Error while connecting to PostgreSQL. Error is ' + str(error))
                    job_update_statement = "UPDATE user_job SET job_status = 'FAILED', modified_on = CURRENT_TIMESTAMP WHERE job_id = (%s)"
                    # Execute the SQL Query
                    meta_db_cursor.execute(job_update_statement, (job_id,))
                    meta_connection.commit()
                finally:
                    # Closing database connection.
                    wos_cursor.close()
                    mag_cursor.close()
                    meta_db_cursor.close()
                    mag_driver_session.close()
                    wos_driver_session.close()
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
