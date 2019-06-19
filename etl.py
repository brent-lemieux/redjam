import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

config = configparser.ConfigParser()
config.read('dwh.cfg')

HOST = config.get("CLUSTER", "HOST")
DBNAME = config.get("CLUSTER", "DBNAME")
USER = config.get("CLUSTER", "USER")
PASSWORD = config.get("CLUSTER", "PASSWORD")
PORT = config.get("CLUSTER", "PORT")


def load_staging_tables(cur, conn):
    """Load data from S3 into the staging tables.

    Arguments:
        cur (psycopg2.cursor) - sql cursor object
        conn (psycopg2.connect) - sql connection object
    """
    for query in copy_table_queries:
        print('Running:\n%s' % query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Transfer data from staging tables to fact and dimension tables for
    analytics queries.

    Arguments:
        cur (psycopg2.cursor) - sql cursor object
        conn (psycopg2.connect) - sql connection object
    """
    for query in insert_table_queries:
        print('Running:\n%s' % query)
        cur.execute(query)
        conn.commit()


def main():
    """Populate the tables with S3 data specified in dwh.cfg."""
    # Create connection and cursor.
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            HOST, DBNAME, USER, PASSWORD, PORT
        )
    )
    cur = conn.cursor()
    # Load staging tables then insert data into the star schema.
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    # Close the connection.
    conn.close()


if __name__ == "__main__":
    main()
