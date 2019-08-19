# import the packages which are needed in this file
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


# define functions
def create_database():
    """
    Description: 
        This function helps us to create a connection object and cursor object for connecting
        to Postgres.
    Parameters: None
    """
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Description: drop tables in Postgres database if they exist    
    Parameters:
        -conn: connection object (with Postgres)
        -cur: cursor object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: create tables in Postgres database   
    Parameters:
        -conn: connection object (with Postgres)
        -cur: cursor object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    print('connect to Postgres...')
    cur, conn = create_database()
    
    print('create tables...')
    create_tables(cur, conn)

    print('close connection...')
    conn.close()


if __name__ == "__main__":
    main()