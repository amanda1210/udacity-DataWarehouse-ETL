import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
    Load raw data from s3 to staging tables
    
    If success, print "load_finish" and "load_commit"
    
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        print("load_finish")
        conn.commit()
        print("load_commit")


def insert_tables(cur, conn):
    
    """
    Insert data that in staging tables to facts and dimension tables to get final tables
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        print('insert_finish')
        conn.commit()
        print('insert_commit')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    """
    Connect to AWS Redshift cluster to execute load_taging_tables function and insert_tables function
    """

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print("connect successed")
    cur = conn.cursor()
    
    """
    If the connection is successful, print "connect successed"
    """
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()