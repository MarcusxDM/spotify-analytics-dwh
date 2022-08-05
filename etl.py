import configparser
from operator import index
import psycopg2
from sql_queries import insert_table_queries, copy_table_queries, insert_countries_table_staging, data_quality_checks
import os
import pandas as pd
    
def load_staging_tables(cur, conn):
    '''
    Loads chart & artist staging tables by executing copy queries defined in copy_table_queries
    from sql_queries.py

        Parameters:
                cur (cursor object): cursor from PostgreSQL connection
                conn (conection object): psycopg2 connection with PostgreSQL
        Returns:
                None
    '''    
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print("### DONE ###")

def load_staging_json(cur, conn, continent, country):
    '''
    Loads countries json files to pandas DataFrame, joins them and loads 
    its staging table by executing insert queries defined in insert_countries_table_staging
    from  from sql_queries.py
    from sql_queries

        Parameters:
                cur (cursor object): cursor from PostgreSQL connection
                conn (conection object): psycopg2 connection with PostgreSQL
                continent (string): continent json file path
                country (string): country json file path
        Returns:
                None
    '''    
    continent_df = pd.read_json(continent, lines=True)
    continent_df = pd.melt(continent_df, var_name='country_id', value_name="continent")

    country_df = pd.read_json(country, lines=True)
    country_df = pd.melt(country_df, var_name='country_id', value_name="country_name")
    
    join_df = pd.merge(country_df, continent_df, on='country_id', how='left')
    print(join_df)

    for index, row in join_df.iterrows():
        cur.execute(insert_countries_table_staging, (row['country_id'], row['country_name'], row['continent']))
    conn.commit()
    print("### DONE ###")
    
def insert_tables(cur, conn):
    '''
    Cleans and filters data from staging tables inserting into each respective table from the star schema
    by executing insert queries defined in insert_table_queries from sql_queries.py

        Parameters:
                cur (cursor object): cursor from PostgreSQL connection
                conn (conection object): psycopg2 connection with PostgreSQL
        Returns:
                None
    '''
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print("### DONE ###")

def check_data_quality(cur):
    '''
    Executes data quality checks defined in data_quality_checks from sql_queries.py

        Parameters:
                cur (cursor object): cursor from PostgreSQL connection
        Returns:
                None
    '''
    i = 0
    for check in data_quality_checks:
        i+=1
        cur.execute(check['sql'])
        result = cur.fetchone()[0]
        if check['type'] == 'query':
            cur.execute(check['expected_result'])
            expected_result = cur.fetchone()[0]
        else:
            expected_result = check['expected_result']
        if result == expected_result:
            print(f"#{i} Data Quality OK")
        else:
            print(f"#{i} Data Quality ERROR")
            

def main():
    '''
    Reads configuration file 'db_connection.cfg', connects to PostgreSQL database with psycopg2 and
    Executes loads for staging tables, multidimensional tables and quality checks

        Parameters:
                None
        Returns:
                None
    '''
    config = configparser.ConfigParser()
    config.read('db_connection.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()
    
    load_staging_json(cur, conn, config['SOURCE']['CONTINENT'], config['SOURCE']['COUNTRY'])
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    check_data_quality(cur)

    conn.close()


if __name__ == "__main__":
    main()