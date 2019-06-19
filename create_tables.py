import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

config = configparser.ConfigParser()
config.read('dwh.cfg')

HOST = config.get("CLUSTER", "HOST")
DBNAME = config.get("CLUSTER", "DBNAME")
USER = config.get("CLUSTER", "USER")
PASSWORD = config.get("CLUSTER", "PASSWORD")
PORT = config.get("CLUSTER", "PORT")


def drop_tables(cur, conn):
    """Drop tables using statements in the drop_table_queries list.

    Arguments:
        cur (psycopg2.cursor) - sql cursor object
        conn (psycopg2.connect) - sql connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create tables using statements in the create_table_queries list.

    Arguments:
        cur (psycopg2.cursor) - sql cursor object
        conn (psycopg2.connect) - sql connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Drop tables if they exist, then create them."""
    # Create connection and cursor.
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            HOST, DBNAME, USER, PASSWORD, PORT
        )
    )
    cur = conn.cursor()
    # Drop the tables if they exist and create them again.
    drop_tables(cur, conn)
    create_tables(cur, conn)
    # Close the connection.
    conn.close()


if __name__ == "__main__":
    main()
