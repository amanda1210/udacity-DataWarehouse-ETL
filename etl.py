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
    Insert data that in staging tables to facts and dimension tables
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
    Connect to AWS Redshift cluster 
    
    """

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print("connect succeed")
    cur = conn.cursor()
    
    """
    Execute load_staging_tables function and insert_tables function
    :param1 cur
    :param2 conn
    """
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()