import configparser
from operator import index
import psycopg2
from sql_queries import insert_table_queries, copy_table_queries, insert_countries_table_staging, data_quality_checks
import os
import pandas as pd

def get_abs_path(filepath):
    return os.path.abspath(filepath)
    
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print("### DONE ###")

def load_staging_json(cur, conn, continent, country):
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
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print("### DONE ###")

def check_data_quality(cur):
    for check in data_quality_checks:
        cur.execute(check['sql'])
        result = cur.fetchone()[0]
        if result == check['expected_result']:
            print("Data Quality OK")
        else:
            print("Data Quality ERROR")

def main():
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