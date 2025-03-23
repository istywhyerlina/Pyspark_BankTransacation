from pyspark.sql import SparkSession
from helper.utils import initiate_spark

from extract.extract import extract_csv
from extract.extract import extract_database
from transform.transform import Transform
import pyspark.sql.functions as f
from load.load import load_dwh



# Inisialisasi SparkSession
spark = initiate_spark()

# handle legacy time parser
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Extract data 
try:
    bank_transaction=extract_csv('data/new_bank_transaction.csv')
    print ('new_bank_transaction.csv Extracted')
    table_names=["education_status","marital_status","marketing_campaign_deposit"]
    for name in table_names:
        globals()[f'{name}']=extract_database(name)
    print ('"education_status","marital_status","marketing_campaign_deposit" Extracted from posgreSQL')
except Exception as e:
    print (e)

# Transform data
try: 
    trans=Transform()
    marketing_campaign_deposit=trans.marketing_campaign_deposit(data=marketing_campaign_deposit)
    print("Transformed marketing_campaign_deposit data")
    
    customers=trans.customers(bank_transaction)
    print("Transformed customers data")
    #col_earlist_date = f.min('birth_date').alias('earliest')
    #col_latest_date = f.max('birth_date').alias('latest')
    
    #df_result = customers.select(col_earlist_date, col_latest_date)
    #df_result.show()  
    transactions=trans.transactions(bank_transaction)
    print("Transformed transactions data")
    
except Exception as e:
    print (e)
    
# Load data
load_dwh(education_status,"education_status",spark=spark)
print("Loaded education_status data")
load_dwh(marital_status,"marital_status",spark=spark)
print("Loaded marital_status data")

load_dwh(marketing_campaign_deposit,"marketing_campaign_deposit",spark=spark)
print("Loaded marketing_campaign_deposit data")

load_dwh(customers,"customers",spark=spark)
print("Loaded customers data")

load_dwh(transactions,"transactions",spark=spark)
print("Loaded transactions data")



