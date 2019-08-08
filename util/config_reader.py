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
        traceback.print_tb(e.__traceback__)
        raise Exception('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')


def get_aws_access_key():
    try:
        config = get_cadre_config()
        access_key = config['AWS']['aws_access_key_id']
        return access_key
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_aws_access_key_secret():
    try:
        config = get_cadre_config()
        access_key_secret = config['AWS']['aws_secret_access_key']
        return access_key_secret
    except Exception as e:
        traceback.print_tb(e.__traceback__)
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
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_package_queue_url():
    try:
        config = get_cadre_config()
        queue_url = config['AWS']['package_queue']
        return queue_url
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_tools_s3_root():
    try:
        config = get_cadre_config()
        s3_root = config['AWS']['tools_s3_root']
        return s3_root
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_archive_s3_root():
    try:
        config = get_cadre_config()
        s3_root = config['AWS']['archive_s3_root']
        return s3_root
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')

def get_wos_db_hostname():
    try:
        config = get_cadre_config()
        db_host_name = config['WOS_DATABASE_INFO']['database-host']
        return db_host_name
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_wos_db_port():
    try:
        config = get_cadre_config()
        db_port = config['WOS_DATABASE_INFO']['database-port']
        return db_port
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_wos_db_name():
    try:
        config = get_cadre_config()
        db_name = config['WOS_DATABASE_INFO']['database-name']
        return db_name
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_wos_db_username():
    try:
        config = get_cadre_config()
        db_username = config['WOS_DATABASE_INFO']['database-username']
        return db_username
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_wos_db_pwd():
    try:
        config = get_cadre_config()
        db_pwd = config['WOS_DATABASE_INFO']['database-password']
        return db_pwd
    except Exception as e:
        traceback.print_tb(e.__traceback__)
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
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_mag_db_name():
    try:
        config = get_cadre_config()
        db_name = config['MAG_DATABASE_INFO']['database-name']
        return db_name
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_mag_db_username():
    try:
        config = get_cadre_config()
        db_username = config['MAG_DATABASE_INFO']['database-username']
        return db_username
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_mag_db_pwd():
    try:
        config = get_cadre_config()
        db_pwd = config['MAG_DATABASE_INFO']['database-password']
        return db_pwd
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_mag_graph_db_url():
    try:
        config = get_cadre_config()
        db_url = config['MAG_GRAPH_DB_INFO']['database-url']
        return db_url
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_mag_graph_db_username():
    try:
        config = get_cadre_config()
        db_username = config['MAG_GRAPH_DB_INFO']['database-username']
        return db_username
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_mag_graph_db_pwd():
    try:
        config = get_cadre_config()
        db_pwd = config['MAG_GRAPH_DB_INFO']['database-password']
        return db_pwd
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_db_hostname():
    try:
        config = get_cadre_config()
        db_host_name = config['CADRE_META_DATABASE_INFO']['database-host']
        return db_host_name
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_db_port():
    try:
        config = get_cadre_config()
        db_port = config['CADRE_META_DATABASE_INFO']['database-port']
        return db_port
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_db_name():
    try:
        config = get_cadre_config()
        db_name = config['CADRE_META_DATABASE_INFO']['database-name']
        return db_name
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_db_username():
    try:
        config = get_cadre_config()
        db_username = config['CADRE_META_DATABASE_INFO']['database-username']
        return db_username
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_db_pwd():
    try:
        config = get_cadre_config()
        db_pwd = config['CADRE_META_DATABASE_INFO']['database-password']
        return db_pwd
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')


def get_cadre_efs_root():
    try:
        config = get_cadre_config()
        efs_root = config['EFS']['efs-root']
        return efs_root
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('Unable to find cadre.config file. Make sure you have cadre.config inside conf directory !')
        raise Exception('Unable to find cadre.config file !')
