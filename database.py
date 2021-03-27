""" database.py
Contains all of the database tools needed to connect and write SQL queries against my databases.
Designed for a Postgresql 11 server, authentication is stored separately
"""

import psycopg2
from pandas import read_csv, read_sql


def create_connection_string(db):
    """
    Requires the 'config' file for credentials
    :param db: database for the connection string
    :return: creates a connection string for connecting to the server
    """
    host = 'raspberrypi:5432'
    conf = read_csv('config', index_col='db').loc[db]
    uid = conf['uid']
    pwd = conf['pwd']

    return f'postgresql://{uid}:{pwd}@{host}/dev01'


def create_SQL(dict_object, table):
    """
    Requires only one value per key
    :param dict_object: A dictionary object which has keys which are equivalent to column names on a table
    :param table: the table which is being written to
    :return: a SQL text string which can be executed
    """
    columns = []
    values = []
    [(columns.append(d[0]), values.append(str(d[1]))) for d in dict_object.items()]
    values = "','".join(values)
    sql = f"INSERT INTO {table} ({', '.join(columns)}) \nVALUES ('{values}')"
    return sql


def execute_query(sql, db='dev01'):
    """
    :param sql: the SQL string to be executed
    :param db: the database to be connected to
    :return: executes the SQL string on the connection supplied. Will not return SELECT results.
    """
    connection_string = create_connection_string(db)

    conn = psycopg2.connect(connection_string)
    crsr = conn.cursor()
    for query in [s for s in sql.split(';') if s != '']:
        crsr.execute(query)
    conn.commit()
    crsr.close()
    conn.close()


def missing_table(table, db='dev01'):
    """
    :param table: table to check the presence of
    :param db: the database to be connected to
    :return: a boolean result if the table is present within the selected table. True if missing, False if present
    """
    table = table.lower()
    connection_string = create_connection_string(db)
    # uses the postgres function to_regclass()
    results = read_sql(f"SELECT to_regclass('public.{table}')", connection_string)
    return results.to_regclass[0] != table
