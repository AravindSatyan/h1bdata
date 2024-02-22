import pandas as pd, datetime as dt, glob, decimal, ijson, time as t, os, json
from pytz import timezone
from bson.decimal128 import Decimal128
from sqlalchemy import create_engine
from pymongo import MongoClient


def pg_db_connection():
        db_username = 'postgres'
        db_password = 'your_password'
        db_host = 'localhost'
        db_port = '5432'
        db_name = 'hariaravi'
        # SQLAlchemy engine for PostgreSQL
        engine = create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
        return engine

def date_and_time():
    format = "%Y-%m-%d_%H-%M"
    now_utc = dt.datetime.now(timezone('EST'))
    return now_utc.strftime(format)

def get_full_data(engine,datetime):
    data_file = f'final/dol-h1b-data-final-{datetime}.json'
    with open (r'{}'.format(data_file), mode='a', encoding = 'utf-8-sig') as file:
        for i in range(20):
            starttime_sql_query=dt.datetime.now()
            query= f'select * from table_is_{i}'
            temp_df = pd.read_sql(query, engine)
            endtime_sql_query = dt.datetime.now()
            temp_df.to_json(file, orient='records')
            end_time_df_json= dt.datetime.now()
            print(f'table is {i}, the shape is: {temp_df.shape}')
            print(f'time to query: {endtime_sql_query-starttime_sql_query}')
            print(f'time to json: {end_time_df_json - endtime_sql_query}')
            file_name = file.name
            abs_path=os.path.abspath(file_name)
            # print(temp_df)
        return abs_path     

def convert_decimal128(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            obj[k] = convert_decimal128(v)
    elif isinstance(obj, list):
        obj = [convert_decimal128(v) for v in obj]
    elif isinstance(obj, decimal.Decimal):
        return Decimal128(str(obj))
    return obj



def load_data(df):
    with open (r'{}'.format(df),mode = 'r', encoding = 'utf-8-sig') as file:
        array_items = ijson.items(file, 'item')
        temp_list=[]
        count=0
        for i in array_items:
            temp_list.append(i)
            if len(temp_list)==10000:
                atlas_connection_string = "mongodb+srv://aravindbedean:25w9gXhCYvSdNYho@cluster0.cbcz4vn.mongodb.net/?retryWrites=true&w=majority"
                client = MongoClient(atlas_connection_string)
                db = client['s1']
                collection = db['s2']
                temp_list=convert_decimal128(temp_list)
                collection.insert_many(temp_list)
                temp_list.clear()
                count+=10000
                print(count)

        #upload the remaining data
        atlas_connection_string = "mongodb+srv://aravindbedean:25w9gXhCYvSdNYho@cluster0.cbcz4vn.mongodb.net/?retryWrites=true&w=majority"
        client = MongoClient(atlas_connection_string)
        db = client['s1']
        collection = db['s2']
        temp_list=convert_decimal128(temp_list)
        collection.insert_many(temp_list)
        temp_list.clear()
        print(len(temp_list))
 
                
def operation():
    pg_db = pg_db_connection()
    temp_variable=get_full_data(pg_db, date_and_time())
    final_end = load_data(temp_variable)
    print('All functions done')

        
# operation()
print('done for docker and the image da hari!')
