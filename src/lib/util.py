"""
    Define miscellaneous functions to be used across the application
"""

from os import environ
import urllib
import logging
from logging import Formatter
import servicemanager
import pyodbc
import sqlalchemy

class MSSQL(object):
    """Wrapper for handling the MSSQL connection"""
    def __init__(self):
        driver = 'ODBC Driver 17 for SQL Server'
        svr = environ['MSSQL_SERVER']
        db = environ['MSSQL_DB']
        uid = environ['MSSQL_UID']
        pwd = environ['MSSQL_PWD']

        self.connection_str = f'DRIVER={driver};SERVER={svr};DATABASE={db};UID={uid};PWD={pwd}'
        self.connection = pyodbc.connect(self.connection_str)

    def __enter__(self):
        return self

    def __exit__(self, exctype, excinst, exctb):
        self.commit()
        self.connection.close()

    def commit(self):
        """executes a commit"""
        return self.execute('commit')

    def cursor(self):
        """gets a cursor"""
        return self.connection.cursor()

    def execute(self, query, params=()):
        '''Executes a query'''
        return self.cursor().execute(query, params)

    def query(self, query, params=()):
        '''Executes a query and returns the results'''
        return self.execute(query, params).fetchall()

    def query_one(self, query, params=()):
        '''Executes a query and returns a result'''
        return self.execute(query, params).fetchone()

    def get_connection(self):
        '''returns the connection object'''
        return self.connection

    def get_engine(self):
        '''returns an sqlalchemy engine'''
        quoted = urllib.parse.quote_plus(self.connection_str)
        return sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}')

class ServiceManagerLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            if record.levelno >= logging.ERROR:
                servicemanager.LogErrorMsg(msg)
            elif record.levelno >= logging.INFO:
                servicemanager.LogInfoMsg(msg)
        except Exception:
            pass

def setup_logger(logger_name):
    formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler = ServiceManagerLogHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger(logger_name)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def setup_file_logger(logger_name):
    logger = logging.getLogger(logger_name)
    path = 'C:\\Users\\Scott\\TradingMonkey_ETL\\TradingMonkey_ETL.log'
    handler = logging.FileHandler(path, encoding='utf-8')
    formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))
