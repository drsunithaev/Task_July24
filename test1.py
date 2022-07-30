import mysql.connector as con
import logging
from log import log_file



def db_connect(h,u,p):
    """
    h : host
    u : username
    p : password
    Create a database connector
    :return: returns the connector
    """
    try:
        logging.info("Connect to database")
        mydb = con.connect(host = h,
                           user=u,
                           password=p,
                           use_pure=True)
        return mydb
    except Exception as e:
        mydb.close()
        print(e)
        logging.exception(e)

def db_create_table(mydb, db_name, table_name, attr_l):
    """
    create a new table in the specified database
    :param mydb: database connector
    :param db_name: database name
    :param table_name: table name
    :return: true/false
    """
    try:
        logging.info(f"Creating a table {table_name} in the db {db_name}")
        cursor = mydb.cursor()
        query = "create table if not exists "+str(db_name)+"."+str(table_name)"

        cursor.execute("create table if not exists "+str(db_name)+"."+str(table_name)(id int(10),company varchar(30), "
                       "start date, "
                       "end date, "
                       "ctc float(10)"
                       "remarks varchar(50))")
        cursor.execute("insert into sunitha.experience values("
                       "002, 'Amrita', 06/10/2004, 30/12/2006, 15000, 'first job'")
        mydb.commit()
        return True
    except Exception as e:
        logging.exception(e)
        return False

def db_create_table_q(mydb, q):
    """
    create a new table in the specified database
    :param mydb: database connector
    :param db_name: database name
    :param table_name: table name
    :return: true/false
    """
    try:
        logging.info(f"Creating a table : {q}")
        cursor = mydb.cursor()
        cursor.execute(q)
       # cursor.execute("insert into sunitha.experience values("
                      # "002, 'Amrita', 06/10/2004, 30/12/2006, 15000, 'first job'")
        mydb.commit()
        return True
    except Exception as e:
        logging.exception(e)
        return False


def db_create_db(mydb, db_name):
    try:
        logging.info(f"creating a table {db_name}")
        query = "create database "+db_name
        cursor = mydb.cursor()
        cursor.execute(query)
    except Exception as e:
        logging.exception(e)

def db_query(mydb,query):
    """
    Execute query
    :param mydb: database connector
    :return: result from database
    """
    try:
        logging.info("query--> SHOW DATABASES;")
        #query = "SHOW DATABASES;"
        cursor = mydb.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        print(type(result))
        #print(cursor.fetchall())
        mydb.close()
        return result
    except Exception as e:
        mydb.close()
        print(e)
        logging.exception(e)

