import logging
import os, traceback, sys
import configparser


abspath = os.path.abspath(os.path.dirname(__file__))
parent = os.path.dirname(abspath)
sys.path.append(parent)

logger = logging.getLogger(__name__)


def get_cadre_config():
    try:
        config_path = parent + '/conf/cadre.config'
        if os.path.isfile(config_path):
            config = configparser.RawConfigParser()
            config.read(config_path)
            return config
        else:

            logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
            raise Exception('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
    except Exception as e:
        logger.exception(e)
        raise Exception('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')


def get_aws_access_key():
    try:
        config = get_cadre_config()
        access_key = config['AWS']['aws_access_key_id']
        return access_key
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_aws_access_key_secret():
    try:
        config = get_cadre_config()
        access_key_secret = config['AWS']['aws_secret_access_key']
        return access_key_secret
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_aws_region():
    try:
        config = get_cadre_config()
        region_name = config['AWS']['region_name']
        return region_name
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_job_queue_url():
    try:
        config = get_cadre_config()
        queue_url = config['AWS']['job_queue']
        return queue_url
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_package_queue_url():
    try:
        config = get_cadre_config()
        queue_url = config['AWS']['package_queue']
        return queue_url
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_tool_queue_url():
    try:
        config = get_cadre_config()
        queue_url = config['AWS']['tool_queue']
        return queue_url
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_tools_s3_root():
    try:
        config = get_cadre_config()
        s3_root = config['PACKAGES']['tools_s3_root']
        return s3_root
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_archive_s3_root():
    try:
        config = get_cadre_config()
        s3_root = config['PACKAGES']['archive_s3_root']
        return s3_root
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')

def get_wos_db_hostname():
    try:
        config = get_cadre_config()
        db_host_name = config['WOS_DATABASE_INFO']['database-host']
        return db_host_name
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_wos_db_port():
    try:
        config = get_cadre_config()
        db_port = config['WOS_DATABASE_INFO']['database-port']
        return db_port
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_wos_db_name():
    try:
        config = get_cadre_config()
        db_name = config['WOS_DATABASE_INFO']['database-name']
        return db_name
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_wos_db_username():
    try:
        config = get_cadre_config()
        db_username = config['WOS_DATABASE_INFO']['database-username']
        return db_username
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_wos_db_pwd():
    try:
        config = get_cadre_config()
        db_pwd = config['WOS_DATABASE_INFO']['database-password']
        return db_pwd
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_mag_db_hostname():
    try:
        config = get_cadre_config()
        db_host_name = config['MAG_DATABASE_INFO']['database-host']
        return db_host_name
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_mag_db_port():
    try:
        config = get_cadre_config()
        db_port = config['MAG_DATABASE_INFO']['database-port']
        return db_port
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_mag_db_name():
    try:
        config = get_cadre_config()
        db_name = config['MAG_DATABASE_INFO']['database-name']
        return db_name
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_mag_db_username():
    try:
        config = get_cadre_config()
        db_username = config['MAG_DATABASE_INFO']['database-username']
        return db_username
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_mag_db_pwd():
    try:
        config = get_cadre_config()
        db_pwd = config['MAG_DATABASE_INFO']['database-password']
        return db_pwd
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_mag_graph_db_url():
    try:
        config = get_cadre_config()
        db_url = config['MAG_GRAPH_DB_INFO']['database-url']
        return db_url
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_mag_graph_db_username():
    try:
        config = get_cadre_config()
        db_username = config['MAG_GRAPH_DB_INFO']['database-username']
        return db_username
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_mag_graph_db_pwd():
    try:
        config = get_cadre_config()
        db_pwd = config['MAG_GRAPH_DB_INFO']['database-password']
        return db_pwd
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_mag_graph_db_import_dir():
    try:
        config = get_cadre_config()
        import_dir = config['MAG_GRAPH_DB_INFO']['import-dir']
        return import_dir
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_wos_graph_db_url():
    try:
        config = get_cadre_config()
        db_url = config['WOS_GRAPH_DB_INFO']['database-url']
        return db_url
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_wos_graph_db_username():
    try:
        config = get_cadre_config()
        db_username = config['WOS_GRAPH_DB_INFO']['database-username']
        return db_username
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_wos_graph_db_pwd():
    try:
        config = get_cadre_config()
        db_pwd = config['WOS_GRAPH_DB_INFO']['database-password']
        return db_pwd
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_wos_graph_db_import_dir():
    try:
        config = get_cadre_config()
        import_dir = config['WOS_GRAPH_DB_INFO']['import-dir']
        return import_dir
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_db_hostname():
    try:
        config = get_cadre_config()
        db_host_name = config['CADRE_META_DATABASE_INFO']['database-host']
        return db_host_name
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_db_port():
    try:
        config = get_cadre_config()
        db_port = config['CADRE_META_DATABASE_INFO']['database-port']
        return db_port
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_db_name():
    try:
        config = get_cadre_config()
        db_name = config['CADRE_META_DATABASE_INFO']['database-name']
        return db_name
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_db_username():
    try:
        config = get_cadre_config()
        db_username = config['CADRE_META_DATABASE_INFO']['database-username']
        return db_username
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_db_pwd():
    try:
        config = get_cadre_config()
        db_pwd = config['CADRE_META_DATABASE_INFO']['database-password']
        return db_pwd
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_efs_root_query_results_listener():
    try:
        config = get_cadre_config()
        efs_root = config['EFS']['efs-root-query-results-listener']
        return efs_root
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_efs_subpath_query_results_listener():
    try:
        config = get_cadre_config()
        efs_subpath = config['EFS']['efs-subpath-query-results-listener']
        return efs_subpath
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_efs_root_neo4j_mag_output_listener():
    try:
        config = get_cadre_config()
        efs_root_datasets = config['EFS']['efs-root-mag-neo4j-import-listener']
        return efs_root_datasets
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_efs_root_neo4j_wos_output_listener():
    try:
        config = get_cadre_config()
        efs_root_datasets = config['EFS']['efs-root-wos-neo4j-import-listener']
        return efs_root_datasets
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')

def get_kebenetes_namespace():
    try:
        config = get_cadre_config()
        kub_namespace = config['PACKAGES']['kub_namespace']
        return kub_namespace
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_pv_name():
    try:
        config = get_cadre_config()
        cadre_pv_name = config['PACKAGES']['cadre-pv-name']
        return cadre_pv_name
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_pvc_name():
    try:
        config = get_cadre_config()
        cadre_pvc_name = config['PACKAGES']['cadre-pvc-name']
        return cadre_pvc_name
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_dockerhub_username():
    try:
        config = get_cadre_config()
        dockerhub_username = config['PACKAGES']['docker-hub-username']
        return dockerhub_username
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_dockerhub_pwd():
    try:
        config = get_cadre_config()
        dockerhub_pwd = config['PACKAGES']['docker-hub-pwd']
        return dockerhub_pwd
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_dockerhub_repo():
    try:
        config = get_cadre_config()
        dockerhub_repo = config['PACKAGES']['docker-repo']
        return dockerhub_repo
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_kub_config_location():
    try:
        config = get_cadre_config()
        kub_config_location = config['PACKAGES']['kub-config-file']
        return kub_config_location
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_kub_docker_secret():
    try:
        config = get_cadre_config()
        kub_docker_secret = config['PACKAGES']['kub-docker-secret']
        return kub_docker_secret
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')