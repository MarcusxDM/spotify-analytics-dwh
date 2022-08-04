import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import os


def drop_tables(cur, conn):
    '''
    Executes drop table queries defined in drop_table_queries from sql_queries.py
    Parameters:
                cur (cursor object): cursor from PostgreSQL connection
                conn (conection object): psycopg2 connection with PostgreSQL
        Returns:
                None
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Executes create table queries defined in create_table_queries from sql_queries.py
    Parameters:
                cur (cursor object): cursor from PostgreSQL connection
                conn (conection object): psycopg2 connection with PostgreSQL
        Returns:
                None
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Reads configuration file 'db_connection.cfg', connects to PostgreSQL database with psycopg2 and
    Executes drop and create table queries

        Parameters:
                None
        Returns:
                None
    '''
    config = configparser.ConfigParser()
    config.read('db_connection.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()