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

wos_connection_pool = None
mag_connection_pool = None
cadre_meta_connection_pool = None
mag_driver = None
wos_driver = None


def initialize():
    global wos_connection_pool, mag_connection_pool, cadre_meta_connection_pool
    global mag_driver, wos_driver

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

    mag_connection_pool = pool.SimpleConnectionPool(1,
                                                    20,
                                                    host=util.config_reader.get_mag_db_hostname(),
                                                    database=util.config_reader.get_mag_db_name(),
                                                    user=util.config_reader.get_mag_db_username(),
                                                    password=util.config_reader.get_mag_db_pwd(),
                                                    port=util.config_reader.get_mag_db_port())

    if mag_connection_pool:
        logger.info("Connection pool for MAG database created successfully")


    cadre_meta_connection_pool = pool.SimpleConnectionPool(1,
                                                    20,
                                                    host=util.config_reader.get_cadre_db_hostname(),
                                                    database=util.config_reader.get_cadre_db_name(),
                                                    user=util.config_reader.get_cadre_db_username(),
                                                    password=util.config_reader.get_cadre_db_pwd(),
                                                    port=util.config_reader.get_cadre_db_port())

    if cadre_meta_connection_pool:
        logger.info("Connection pool for cadre meta database created successfully")


    mag_driver = GraphDatabase.driver(util.config_reader.get_mag_graph_db_url(),
                                      auth=(util.config_reader.get_mag_graph_db_username(), util.config_reader.get_mag_graph_db_pwd()),
                                      max_connection_lifetime=3600*24*30, keep_alive=True)

    wos_driver = GraphDatabase.driver(util.config_reader.get_wos_graph_db_url(),
                                      auth=(util.config_reader.get_wos_graph_db_username(), util.config_reader.get_wos_graph_db_pwd()),
                                      max_connection_lifetime=3600*24*30, keep_alive=True)