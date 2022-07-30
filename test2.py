import json
import mysql.connector as con
import logging
from log import log_file
import pandas as pd
from sqlalchemy import create_engine
import pymongo

def db_connect():
    """
    Connect to database
    :return:
    """
    try:
        logging.info("Connect to database")
        mydb = con.connect(host="localhost",
                           user="root",
                           password="root",
                           use_pure=True)
        return  mydb
    except Exception as e:
        mydb.close()
        logging.exception(e)
def create_db(mydb):
    """
    create database
    :param mydb:
    :return:
    """
    try:
        logging.info("create database")
        cursor = mydb.cursor()
        query1 = "create database showroom;"
        cursor.execute(query1)
        cursor.execute("use showroom;")
        return True
    except Exception as e:
        logging.exception(e)
        return False

#Create a Table of attribute dataset and dress dataset in mysql workbench/python
def create_tables(mydb):

    """
    create a Table of attribute dataset and dress dataset in mysql workbench/python
    :return:

    """
    try:
        logging.info("create a Table of attribute dataset and dress dataset in mysql workbench/python")
        cursor = mydb.cursor()
        query1 = "create table showroom.attribute_datasets(Dress_ID INT,Style varchar(10),Price varchar(10),Rating decimal (2,1), Size varchar(5),Season varchar(10), NeckLine varchar(10),SleeveLength varchar(15),waiseline varchar(15),Material varchar(30),FabricType varchar(30),Decoration	varchar(20),PatternType varchar(20),Recommendation INT);"
        query2 = "create table showroom.dress_sales (" \
                 "Dress_ID INT,	'29.8.2013' int, '31.8.2013' int, '2.9.2013' int, '4.9.2013' int," \
                 "'6.9.2013' int, '8.9.2013'  int,   '10.9.2013' int, '12.9.2013' int, '14.9.2013' int," \
                 "'16.9.2013' int, '18.9.2013' int, '20.9.2013' int, '22.9.2013' int, '24.9.2013' int," \
                 "'26.9.2013' int, '28.9.2013' int, '30.9.2013' int, '2.10.2013' int, '4.10.2013' int," \
                 "'6.10.2013' int, '8.10.2013' int, '10.10.2013' int, '12.10.2013 int' );"

        cursor.execute(query1)
        cursor.execute(query2)
        mydb.commit()
        return True
    except Exception as e:
        logging.exception(e)
        return False

#Do a bulk load for these 2 tables for respective dataset in mysql workbench/python
def insert_table_bulk():
    """
    insert data into tables from excel files
    :return:
    """
    try:
        logging.info("insert data to table from excel sheet")

        engine = create_engine(
            "mysql+pymysql://" + "root" + ":" + "root" + "@" + "localhost" + ":" + "3306" + "/" + "showroom" + "?" + "charset=utf8mb4")
        conn = engine.connect()

        #insert into attribute_datasets table
        excel_file = pd.ExcelFile('D:\Sunitha\iNeuron\Data sets\\for pandas ex\Attribute DataSet.xlsx')
        excel_dataframe = excel_file.parse(sheet_name="Sheet1")
        #excel_dataframe = excel_dataframe.astype(object).where(pd.notnull(excel_dataframe), None)
        excel_dataframe.to_sql("attribute_datasets", conn, if_exists="append",index=False)

        #insert into dress_sales table
        excel_file = pd.ExcelFile("D:\Sunitha\iNeuron\Data sets\\for pandas ex\Dress Sales.xlsx")
        excel_dataframe = excel_file.parse(sheet_name="Sheet1")
        #excel_dataframe = excel_dataframe.astype(object).where(pd.notnull(excel_dataframe), None)
        excel_dataframe.to_sql("dress_sales", conn, if_exists="append",index=False)


    except Exception as e:
        logging.exception(e)



#Read these datasets in pandas as a dataframe
def read_from_db_pandas():
    """
    read data from databse using pandas
    :return:
    """
    try:
        logging.info("Read data from the database tables 'attribute_datasets' and 'dress_sales'")
        engine = create_engine(
            "mysql+pymysql://" + "root" + ":" + "root" + "@" + "localhost" + ":" + "3306" + "/" + "showroom" + "?" + "charset=utf8mb4")
        conn = engine.connect()
        df = pd.read_sql("select * from attribute_datasets", conn, index_col=None)
        df = pd.DataFrame(df, columns = ['Dress_ID', 'Style', 'Price','Rating','Size','Season','Neckline',
                                        'Sleevelength','Waiseline','Material','Fabrictype','Decoration',
                                        'PatternType','Recommendation'])

        # convert attribute_datasets to json format
        #attr_data_json = df.to_json( orient='records')
        df.to_json('temp1.json', orient='records', lines=True)
        #attr_data_json = df.to_json(index=False, orient='records')
       # print("JSON : ",type(attr_data_json))
        #print(attr_data_json)
       # a = attr_data_json
        #a = json.load(attr_data_json)
        logging.info("Attribute Data Sets\n")
        #print("type of a :", type(a))
        logging.info(df)


        df = pd.read_sql("select * from dress_sales", conn, index_col=None)



        df = pd.DataFrame(df,
                                   columns=[   'Dress_ID', '29.8.2013', '31.8.2013', '2.9.2013', '4.9.2013',
                                                '6.9.2013', '8.9.2013'  ,  '10.9.2013', '12.9.2013', '14.9.2013',
                                                '16.9.2013', '18.9.2013', '20.9.2013', '22.9.2013', '24.9.2013',
                                                '26.9.2013', '28.9.2013', '30.9.2013', '2.10.2013', '4.10.2013',
                                                '6.10.2013', '8.10.2013', '10.10.2013', '12.10.2013'])

        #convert dress_sales to json format
        #dress_data_json = df.to_json( orient='records')
        df.to_json('temp2.json', orient='records', lines=True)
        #d = dress_data_json
        #d = json.load(dress_data_json)
        #print("type of d",type(d))
        logging.info("Dress Sales\n")
        logging.info(df)
        #return a,d
    except Exception as e:
        logging.exception(e)



#Store this json dataset into mongodb (Insert_many will be used)
def store_json_mongo():
    """
    store the json file to mongo db
    :param a - attribute_datasets
    :param d - dress_sales
    :return:
    """
    try:

        logging.info("Connecting to MongoDB server")
        client = pymongo.MongoClient(
            "mongodb")
        db = client.test
        logging.info("Create DB showroom")
        # create a databse 'showroom'
        database1 = client['showroom']

        logging.info("Create collection attribute_datasets")
        # create a collection (table)
        collection_a = database1['attribute_datasets']

        logging.info("Create collection dress_sales")
        # create a collection (table)
        collection_d = database1['dress_sales']

        logging.info("inserting json data attribute_datasets and dress_sales to MongoDB")
        '''
        with open('temp1.json') as file:
            file_data1 = json.load(file)
            collection_a.insert_many(file_data1)
        with open('temp2.json') as file:
            file_data2 = json.load(file)
            collection_d.insert_many(file_data2)
        '''


        file_1 = [json.loads(line) for line in open('temp1.json', 'r')]
        collection_a.insert_many(file_1)

        file_2 = [json.loads(line) for line in open('temp2.json', 'r')]
        collection_d.insert_many(file_2)


    except Exception as e:
        logging.exception(e)

#In SQL task, try to perform left join operation with Attribute dataset and dress dataset on column Dress_ID

def left_join_dressID():
    """
    left join attribute_datasets table and dress_sales table
    :return:
    """
    try:
        logging.info("Left join :> attribute_datasets & dress_sales tables")
        engine = create_engine(
            "mysql+pymysql://" + "root" + ":" + "root" + "@" + "localhost" + ":" + "3306" + "/" + "showroom" + "?" + "charset=utf8mb4")
        conn = engine.connect()
        df = pd.read_sql("select * from attribute_datasets", conn, index_col=None)
        attr_dt = pd.DataFrame(df, columns=['Dress_ID', 'Style', 'Price', 'Rating', 'Size', 'Season', 'Neckline',
                                       'Sleevelength', 'Waiseline', 'Material', 'Fabrictype', 'Decoration',
                                       'PatternType', 'Recommendation'])
        attr_dt.to_json('temp1.json', orient='records', lines=True)

        df = pd.read_sql("select * from dress_sales", conn, index_col=None)
        dress_sl = pd.DataFrame(df,
                          columns=['Dress_ID', '29.8.2013', '31.8.2013', '2.9.2013', '4.9.2013',
                                   '6.9.2013', '8.9.2013'  ,  '10.9.2013', '12.9.2013', '14.9.2013',
                                   '16.9.2013', '18.9.2013', '20.9.2013', '22.9.2013', '24.9.2013',
                                   '26.9.2013', '28.9.2013', '30.9.2013', '2.10.2013', '4.10.2013',
                                   '6.10.2013', '8.10.2013', '10.10.2013', '12.10.2013'])

        dress_sl.to_json('temp2.json', orient='records', lines=True)
        # left_df.merge(right_df, on='user_id', how='left')
        print(attr_dt.merge(dress_sl, on='Dress_ID', how='left'))
        logging.info(attr_dt.merge(dress_sl, on='Dress_ID', how='left'))
    except Exception as e:
        logging.exception(e)

#Write the SQL query to find out how many unique dress that we have based on Dress_ID
def unique_dress():
    '''
    Count of unique dress
    :return:
    '''
    try:
        logging.info("Unique dress details")
        mydb = con.connect(host="Localhost", user="root", passwd="root", use_pure=True)
        cursor = mydb.cursor()
        cursor.execute("select count(distinct(Dress_ID)) from showroom.attribute_datasets")
        for i in cursor.fetchall():
            print(i)
            logging.info(f"unique dress count {i}")
    except Exception as e:
        logging.exception(e)
#Try to find out how many dress is having recommendation as 0
def recommend_0():
    '''
    Count of dress having recommendation = 0
    :return:
    '''
    try:
        logging.info("Unique dress details")
        mydb = con.connect(host="Localhost", user="root", passwd="root", use_pure=True)
        cursor = mydb.cursor()
        cursor.execute("select count(*) from showroom.attribute_datasets where recommendation = 0")
        for i in cursor.fetchall():
            print(i)
            logging.info(f"unique dress count {i}")
    except Exception as e:
        logging.exception(e)

#Try to find out total dress sales for each and every Dress_ID
def dress_sales_total():
    '''
    Total sales for each dress
    :return:
    '''
    try:
        logging.info("Left join :> attribute_datasets & dress_sales tables")
        engine = create_engine(
            "mysql+pymysql://" + "root" + ":" + "root" + "@" + "localhost" + ":" + "3306" + "/" + "showroom" + "?" + "charset=utf8mb4")
        conn = engine.connect()

        df = pd.read_sql("select * from dress_sales", conn, index_col=None)
        df = df.astype(object).where(pd.notnull(df), None)
        dress_sl = pd.DataFrame(df,
                          columns=['Dress_ID', '29.8.2013', '31.8.2013', '2.9.2013', '4.9.2013',
                                   '6.9.2013', '8.9.2013', '10.9.2013', '12.9.2013', '14.9.2013',
                                   '16.9.2013', '18.9.2013', '20.9.2013', '22.9.2013', '24.9.2013',
                                   '26.9.2013', '28.9.2013', '30.9.2013', '2.10.2013', '4.10.2013',
                                   '6.10.2013', '8.10.2013', '10.10.2013', '12.10.2013'])
        dress_sl.fillna(0)
        dress_sl['total'] = dress_sl[['29.8.2013', '31.8.2013', '2.9.2013', '4.9.2013',
                                      '6.9.2013', '8.9.2013',  '10.9.2013', '12.9.2013', '14.9.2013',
                                      '16.9.2013', '18.9.2013', '20.9.2013', '22.9.2013', '24.9.2013',
                                      '26.9.2013', '28.9.2013', '30.9.2013', '2.10.2013', '4.10.2013',
                                      '6.10.2013', '8.10.2013', '10.10.2013', '12.10.2013']].agg('sum', axis=1)

        logging.info(dress_sl.groupby('Dress_ID')['total'].sum())

    except Exception as e:
        logging.exception(e)


