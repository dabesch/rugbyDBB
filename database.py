""" database.py
Contains all of the database tools needed to connect and write SQL queries against my databases.
Designed for a Postgresql 11 server, authentication is stored separately in the file 'config'
"""

import psycopg2
from pandas import read_csv, read_sql


def conString(db='dev01'):
    """
    Requires the 'config' file for credentials
    :param db: database for the connection string
    :return: creates a connection string for connecting to the server
    """
    host = 'raspberrypi:5432'
    conf = read_csv('../config', index_col='db').loc[db]
    uid = conf['uid']
    pwd = conf['pwd']

    return f'postgresql://{uid}:{pwd}@{host}/{db}'


def createSQL(dictObject, table):
    """
    Requires only one value per key
    :param dictObject: A dictionary object which has keys which are equivalent to column names on a table
    :param table: the table which is being written to
    :return: a SQL text string which can be executed
    """
    columns = []
    values = []
    for col, val in dictObject.items():
        columns.append(col)
        if type(val) == str:
            # Handle errors with apostrophes
            val = val.replace("'", "''")
        values.append(str(val))

    values = "','".join(values)
    sql = f"INSERT INTO {table} ({', '.join(columns)}) \nVALUES ('{values}')"
    return sql


def executeQuery(sql, db='dev01'):
    """
    :param sql: the SQL string to be executed
    :param db: the database to be connected to
    :return: exectutes the SQL string on the connection supplied. Will not return SELECT results.
    """
    connectionString = conString(db)

    conn = psycopg2.connect(connectionString)
    crsr = conn.cursor()
    for query in [s for s in sql.split(';') if s != '']:
        crsr.execute(query)
    conn.commit()
    crsr.close()
    conn.close()


def missingTable(table, db='dev01'):
    """
    :param table: table to check the presence of
    :param db: the database to be connected to
    :return: a boolean result if the table is present within the selected table. True if missing, False if present
    """
    table = table.lower()
    connectionString = conString(db)
    # uses the postgres function to_regclass()
    results = read_sql(f"SELECT to_regclass('public.{table}')", connectionString)
    return results.to_regclass[0] != table


def writeDataFrame(df, table):
    """
    :param df: A pandas dataframe object with columns which match the output table in the database
    :param table: name of table being written to
    :param db: the database
    :return: writes the contents of a dataframe to an existing table
    """
    for i, row in df.iterrows():
        entry = dict(row)
        # write row
        sql = createSQL(entry, table)
        executeQuery(sql)
