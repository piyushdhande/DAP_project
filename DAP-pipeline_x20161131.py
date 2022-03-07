from dagster import pipeline, solid
import requests
import json
import pandas as pd
from sodapy import Socrata
import pymongo
from pymongo import MongoClient
from sodapy import Socrata
import sys
import pymysql
import sqlalchemy
from sqlalchemy import create_engine


@solid
def insert_into_mysql(context, df_mongodb):
    sqlEngine = create_engine(
        'mysql+pymysql://BlueExorcist:1qaz2wsx3EDC4rfv5tgb@dapmysql.c1tewkx6mir7.eu-west-1.rds.amazonaws.com/NYC_DATA')
    dbConnection = sqlEngine.connect()

    try:

        frame1 = df_mongodb.to_sql('Crime', dbConnection, if_exists='fail');
        context.log.info(f"MySQL insertion completed")
    except Exception as ex:
        print(ex)


@solid
def FetchAPIData(context):
    try:
        Client = Socrata("data.cityofnewyork.us", None)  # Connect Socrata
        Results = Client.get("5ebm-myj7", limit=17000)
        context.log.info()

        return Results  # Return the data
    except Exception as exception:
        return exception  # Return the exception

@solid
def DATA_EXTRACTION_API_Crime(context):
    try:
        Client = Socrata("data.cityofnewyork.us", None)  # Connect Socrata
        Crime = Client.get("833y-fsy8", limit=17000)
        context.log.info(f"{Crime}")
        return Crime


    except Exception as exception:

        return exception  # Return the exception



@solid
def ConnectMongoDB(context, db_coll_names_dict):
    try:
        MongoConnection = MongoClient("mongodb://localhost:27017/")  # Making Connection
        Mongodb = MongoConnection[db_coll_names_dict['DB_Name']]  # Create database
        MongoCollection = Mongodb[db_coll_names_dict['Collection_Name']]  # Create collection
        return MongoCollection
    except Exception as exception:
        return exception  # Return the exception


@solid
def ConnectMongoDB_crime(context, db_coll_names_dict):
    try:
        MongoConnection_crime = MongoClient("mongodb://localhost:27017/")  # Making Connection
        Mongodb_crime = MongoConnection_crime[db_coll_names_dict['DB_Name']]  # Create database
        MongoCollection_crime = Mongodb_crime[db_coll_names_dict['Collection_Name']]  # Create collection
        return MongoCollection_crime
    except Exception as exception:
        return exception  # Return the exception





@solid
def get_url_Limit(context):
    return {'URL': "5ebm-myj7", 'Limit': 17000}


@solid
def get_url_Limit_crime(context):
    return {'URL': "833y-fsy8", 'Limit': 17000}






@solid
def fetch_from_mongoDB(context, param):
    MongoConnection = MongoClient('mongodb://localhost:27017/')  # Making Connection
    Mongodb = MongoConnection['test']  # Create database
    MongoCollection = Mongodb['crime_data']
    DF_MongoDB = pd.DataFrame(MongoCollection.find())
    context.log.info(f"** length of list_of_all_data from MongoDB :  {DF_MongoDB} ")
    return DF_MongoDB

@solid
def fetch_from_mongoDB_crime(context, param):
    MongoConnection = MongoClient('mongodb://localhost:27017/')  # Making Connection
    Mongodb= MongoConnection['NYC_crime']  # Create database
    MongoCollection = Mongodb['crime_Data']
    DF_MongoDB_crime = pd.DataFrame(MongoCollection.find())
    context.log.info(f"** length of list_of_all_data from MongoDB :  {DF_MongoDB_crime} ")
    return DF_MongoDB_crime




@solid
def insert_into_mongoDB(context, data):
    MongoConnection = MongoClient('mongodb://localhost:27017/')  # Making Connection
    Mongodb = MongoConnection['test_12']  # Create database
    MongoCollection = Mongodb['house_Data_1']

    records = json.loads(pd.DataFrame.from_dict(data).T.to_json()).values()
    MongoCollection.insert(records)
    context.log.info(f"** Insertion completed ")
    return True

@solid
def insert_into_mongoDB_crime(context, data):
    MongoConnection = MongoClient('mongodb://localhost:27017/')  # Making Connection
    Mongodb_crime = MongoConnection['test']  # Create database
    MongoCollection = Mongodb_crime['crime_data']

    records_crime = json.loads(pd.DataFrame.from_dict(data).T.to_json()).values()
    MongoCollection.insert(records_crime)
    context.log.info(f"** Insertion completed ")
    return True



@solid
def processing_data(context, df_mongodb):
    context.log.info(f"** Checking for nan values :  {df_mongodb.isna().sum()} ")
    df_mongodb.drop(['_id', 'geocoded_column', 'occur_time', ':@computed_region_efsh_h5xi', ':@computed_region_f5dn_yrer',
              ':@computed_region_yeji_bk3q', ':@computed_region_92fq_4b7q', ':@computed_region_sbqj_enih',
              'perp_age_group', 'perp_sex', 'perp_race', 'location_desc'], axis=1, inplace=True)
    df_mongodb.rename(columns={'boro': 'borough'}, inplace=True)
    df_mongodb['occur_date'] = pd.to_datetime(df_mongodb['occur_date'])
    df_mongodb.drop(['vic_race'], axis=1, inplace=True)
    context.log.info(f"** Checking for nan values :  {df_mongodb.isna().sum()} ")

    return df_mongodb

@pipeline
def DAP_Pipeline():
    # insert_into_mysql(fetch_from_mongoDB(insert_into_mongoDB(ConnectMongoDB(FetchAPIData()))))
    insert_into_mysql(processing_data(fetch_from_mongoDB(insert_into_mongoDB_crime(DATA_EXTRACTION_API_Crime()))))