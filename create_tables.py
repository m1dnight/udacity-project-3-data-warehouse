import configparser
import psycopg2

from etl import connectionString, readConfigurationFile, connectToDatabase
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all the tables in the cluster if they exist.
    The queries are specified in sql_queries.py.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create all the tables in the cluster.
    The queries are specified in sql_queries.py.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = readConfigurationFile()
    cs = connectionString(config)
    conn, cur = connectToDatabase(cs)

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
