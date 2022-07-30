import mysql.connector as con
from test2 import *

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    mydb = db_connect()
    create_db(mydb)
    create_tables(mydb)
    insert_table_bulk()
    read_from_db_pandas()
    left_join_dressID()
    unique_dress()
    recommend_0()
    store_json_mongo()
    dress_sales_total()



