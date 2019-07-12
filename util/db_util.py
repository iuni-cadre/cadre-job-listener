import logging
import os
import sys

from psycopg2 import pool
from neo4j import GraphDatabase

abspath = os.path.abspath(os.path.dirname(__file__))
parent = os.path.dirname(abspath)
util = parent + '/util'
sys.path.append(parent)

import util.config_reader

logger = logging.getLogger(__name__)


wos_connection_pool = pool.SimpleConnectionPool(1,
                                                20,
                                                host=util.config_reader.get_wos_db_hostname(),
                                                database=util.config_reader.get_wos_db_name(),
                                                user=util.config_reader.get_wos_db_username(),
                                                password=util.config_reader.get_wos_db_pwd(),
                                                port=util.config_reader.get_wos_db_port())

if wos_connection_pool:
    logger.info("Connection pool for WOS database created successfully")


cadre_meta_connection_pool = pool.SimpleConnectionPool(1,
                                                20,
                                                host=util.config_reader.get_cadre_db_hostname(),
                                                database=util.config_reader.get_cadre_db_name(),
                                                user=util.config_reader.get_cadre_db_username(),
                                                password=util.config_reader.get_cadre_db_pwd(),
                                                port=util.config_reader.get_cadre_db_port())

if cadre_meta_connection_pool:
    logger.info("Connection pool for cadre meta database created successfully")


driver = GraphDatabase.driver(util.config_reader.get_mag_graph_db_url(), auth=(util.config_reader.get_mag_graph_db_username(), util.config_reader.get_mag_graph_db_pwd()))