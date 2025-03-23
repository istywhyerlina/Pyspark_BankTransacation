import pyspark
from pyspark.sql import SparkSession
import os
import pandas as pd
import sys
sys.path.insert(0, '/home/jovyan/work')
from helper.utils import db_connection
from helper.utils import connection_properties
from helper.utils import initiate_spark
import logging
from helper.utils import logging_process



# Inisialisasi SparkSession
spark = initiate_spark()

# handle legacy time parser
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
logging_process()

def extract_csv(filename,spark=spark):
    try: 
        logging.info(f"===== Start Extracting {filename} data =====")
        df=spark.read.csv(filename,header=True)
        logging.info(f"===== Success Extracting {filename} data =====")
        return df
    except Exception as e:
        logging.error(f"====== Failed to Extract Data {filename} ======")
        logging.error(e)


def extract_database(table_name,spark=spark):
    try:
        logging.info(f"===== Start Extracting {table_name} data =====")
        cp_src,_=connection_properties()
        src_url,_ = db_connection()
        df_metadata=spark.read.jdbc(src_url, table=table_name, properties=cp_src)
        logging.info(f"===== Success Extracting {table_name} data =====")
        return df_metadata
    except Exception as e:
        logging.error(f"====== Failed to Extract Data {table_name} ======")
        logging.error(e)
