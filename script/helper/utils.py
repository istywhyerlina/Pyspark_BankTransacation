from pyspark.sql import SparkSession
import logging




src_container = "source_db_container"
src_database = "source"
src_host = "localhost"
src_user = "postgres"
src_password = "postgres"
src_container_port = 5432

dwh_container = "data_warehouse_container"
dwh_database = "data_warehouse"
dwh_host = "localhost"
dwh_user = "postgres"
dwh_password = "postgres"
dwh_container_port = 5432

def db_connection():     
        #src_conn = f'postgresql://{src_user}:{src_password}@{src_host}:{src_port}/{src_database}'
        src_url = f'jdbc:postgresql://{src_container}:{src_container_port}/{src_database}'
        dwh_url = f'jdbc:postgresql://{dwh_container}:{dwh_container_port}/{dwh_database}'

        return src_url, dwh_url

def connection_properties():
    cp_src={"user":src_user,"password":src_password,"driver":"org.postgresql.Driver"}
    cp_dwh={"user":dwh_user,"password":dwh_password,"driver":"org.postgresql.Driver"}
    return cp_src,cp_dwh

def initiate_spark():
    # Inisialisasi SparkSession
    spark = SparkSession.builder.appName("BankTransaction").getOrCreate()
    return spark


def logging_process():
    # Configure logging
    logging.basicConfig(
        filename="/home/jovyan/work/log/info.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )