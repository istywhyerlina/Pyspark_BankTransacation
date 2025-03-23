import pyspark
from pyspark.sql import SparkSession
import os
import pandas as pd
import sys
sys.path.insert(0, '/home/jovyan/work')
from helper.utils import db_connection
from helper.utils import connection_properties
from helper.utils import initiate_spark
from extract.extract_dwh import extract_dwh as extract_dwh
import logging
from helper.utils import logging_process
from sqlalchemy import *
import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError

# Inisialisasi SparkSession
spark = initiate_spark()

# handle legacy time parser
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
logging_process()

def load_dwh(data,table_name:str,spark=spark):
    try:
        logging.info(f"===== Create connection to dwh =====")
        dwh_host="data_warehouse"
        dwh_port=5432
        dwh_password="postgres"
        dwh_user = "postgres"
        dwh_database="data_warehouse"
        dwh_conn = f'postgresql://{dwh_user}:{dwh_password}@{dwh_host}:{dwh_port}/{dwh_database}'
        dwh_engine = create_engine(dwh_conn)
        conn=dwh_engine.connect()
        meta = MetaData()
        meta.reflect(bind=dwh_engine)
        logging.info(f"===== Connection created =====")

        logging.info(f"===== Get only new data =====")
        primary_key={'education_status':'education_id','marital_status':'marital_id','marketing_campaign_deposit':'loan_data_id','customers':'customer_id','transactions':'transaction_id'}
        
        data_in_dwh=extract_dwh(table_name)
        #data_in_dwh.show()
        #data.show()
        new_data=data.join(data_in_dwh, primary_key[table_name],"leftanti")
        #new_data.show()
        
        logging.info(f"===== Already got only New Data  =====")

        logging.info(f"===== Start Loading {table_name} new data =====")
        _,dwh_url = db_connection()
        _,cp_dwh=connection_properties()
        
        new_data.write.jdbc(url=dwh_url,table=table_name,mode="append",properties=cp_dwh)
        logging.info(f"===== Success Loading {table_name} new data =====")
    except Exception as e:
        logging.error(f"====== Failed to Load Data {table_name} ======")
        logging.error(e)
