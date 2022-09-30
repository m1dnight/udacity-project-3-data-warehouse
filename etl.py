import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Fetches the data from the S3 buckets and inserts it into the staging tables.
    Queries are defined in sql_queries.py.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Populates the dimension tables from the staging tables above.
    Queries are defined in sql_queries.py.
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def connectionString(config):
    """
    Creates a connection string to Redshift, based on the values from
    our config file dwh.cfg.
    Be sure to supply the proper HOST value in the config before running!
    """
    host = config.get("DWH", "HOST")
    dbname = config.get("DWH", "DWH_DB")
    dbuser = config.get("DWH", "DWH_DB_USER")
    dbpass = config.get("DWH", "DWH_DB_PASSWORD")
    dbport = config.get("DWH", "DWH_PORT")
    return "host={} dbname={} user={} password={} port={}".format(host, dbname, dbuser, dbpass, dbport)


def readConfigurationFile():
    """
    Reads the configuration file from disk and parses it.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    return config


def connectToDatabase(connectionString):
    """
    Connects to the database using the provided connection string.
    """
    conn = psycopg2.connect(connectionString)
    cur = conn.cursor()
    return conn, cur


def main():
    config = readConfigurationFile()
    cs = connectionString(config)
    conn, cur = connectToDatabase(cs)

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
