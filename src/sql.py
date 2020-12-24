"""Snowflake Utils: Provides a wrapper for the snowflake connector"""

from os import environ
import pyodbc
import sqlalchemy
import urllib

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

    def execute(self, query):
        '''Executes a query'''
        return self.cursor().execute(query)

    def query(self, query):
        '''Executes a query and returns the results'''
        return self.execute(query).fetchall()

    def query_one(self, query):
        '''Executes a query and returns a result'''
        return self.execute(query).fetchone()

    def get_connection(self):
        '''returns the connection object'''
        return self.connection

    def get_engine(self):
        '''returns an sqlalchemy engine'''
        quoted = urllib.parse.quote_plus(self.connection_str)
        return sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}')