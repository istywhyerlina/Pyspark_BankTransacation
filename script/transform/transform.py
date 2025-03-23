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
import pyspark.sql.functions as f
import pyspark.sql.types as sparktypes
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import translate
from pyspark.sql.functions import col
from pyspark.sql.functions import split
from pyspark.sql.functions import when
from pyspark.sql.functions import concat
from pyspark.sql.functions import lit
from pyspark.sql.functions import to_date
from pyspark.sql.functions import *
import datetime as dt
import time

logging_process()


class Transform:
    # Inisialisasi SparkSession
    spark = initiate_spark()
    
    # handle legacy time parser
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    def marketing_campaign_deposit (self, data):
        try:
            logging.info(f"===== Start Transforming {data} data =====")
            #Rename cols
            RENAME_COLS = {
                        "pdays": "days_since_last_campaign",
                        "previous": "previous_campaign_contacts",
                        "poutcome": "previous_campaign_outcome"
                        }
            self.marketing_campaign_deposit = data.withColumnsRenamed(colsMap = RENAME_COLS)
            logging.info(f"Renamed Columns")
            # For balance column: Remove '$' cast to integer
            
            self.marketing_campaign_deposit = self.marketing_campaign_deposit.withColumn("balance", regexp_replace(f.col("balance"), "[$]", ""))
            logging.info(f"Removed '$' in balance column")
            self.marketing_campaign_deposit = self.marketing_campaign_deposit.withColumn("balance",self.marketing_campaign_deposit["balance"].cast("int"))
            logging.info(f"Casted balance column to integer")
            # Create column duration in year 
            self.marketing_campaign_deposit = self.marketing_campaign_deposit.withColumn("duration_in_year",f.floor((self.marketing_campaign_deposit["duration"].cast("int"))/365))
            logging.info(f"Converted duration in year")
            #cast duration column to int
            self.marketing_campaign_deposit = self.marketing_campaign_deposit.withColumn("duration",self.marketing_campaign_deposit["duration"].cast("int"))
            logging.info(f"Casted duration into intefer")
            logging.info(f"===== Finish Transforming {data} data =====")
            return self.marketing_campaign_deposit
        except Exception as e:
            logging.error(f"====== Failed to Transform Data {data} ======")
            logging.error(e)
        
    def customers (self,data):
        try:
            logging.info(f"===== Start Transforming {data} data =====")
            column = ["CustomerID","CustomerDOB","CustGender","CustLocation","CustAccountBalance"]
            self.table_customer = data.select([data[col] for col in column])
            logging.info(f" Selected columns from Bank transaction data ")
            RENAME_COLS = {
                "CustomerID": "customer_id",
                "CustomerDOB": "birth_date",
                "CustGender": "gender",
                "CustLocation": "location",
                "CustAccountBalance": "account_balance"}
            self.table_customer = self.table_customer.withColumnsRenamed(colsMap = RENAME_COLS)
            logging.info(f" Renamed Columns ")
            #Mapping gender column 
            self.table_customer = self.table_customer.withColumn("gender",when(self.table_customer.gender == "M", "Male")
                                   .when(self.table_customer.gender == "F", "Female").otherwise("Other"))
            logging.info(f" Mapped Gender Column ")
            #casting account_balance 
            self.table_customer  = self.table_customer .withColumn("account_balance",self.table_customer ["account_balance"].cast("float"))
            logging.info(f"Casted account_balance Column into float")
            #cast birth_date column to date
            self.table_customer = self.table_customer.withColumn('birth_date', when(col('birth_date').isNull(), dt.date(1800, 1, 1)).otherwise(col('birth_date')))
            self.table_customer= self.table_customer.withColumn('birth_date', to_date( self.table_customer['birth_date'], "d/M/yy"))
            logging.info(f"Casted birth_date Column into date format")
            
            # manipulate data when birthday_date>2025
    
            ''''compare birth_date with earliest transaction date, if birth_date>earlies transaction date, birth_date is invalid. change the value to '1800-01-01' so operational team can ask the customer to re-fill the birth_date information'''
            data=data.withColumn('TransactionDate', to_date(data['TransactionDate'], "d/M/yy"))
            tx_date= data.groupBy('CustomerID').agg(f.min('TransactionDate').alias("EarliestTransactionDate"))
            tx_date=tx_date.withColumnRenamed("CustomerID", "customer_id")
            bd_date= self.table_customer.groupBy('customer_id').agg(f.min('birth_date').alias("birth_date"))
            customer_birtday_transaction=tx_date.join(bd_date, 'customer_id',"right").select(tx_date.customer_id,tx_date.EarliestTransactionDate, bd_date.birth_date)
    
    
            customer_birtday_transaction = customer_birtday_transaction.withColumn("birth_date", when(customer_birtday_transaction.EarliestTransactionDate < 
                                                                                                      customer_birtday_transaction.birth_date,
                                                                                                      dt.date(1800, 1, 1)).otherwise(col("birth_date")))
    
            self.table_customer=self.table_customer.drop(self.table_customer.birth_date)
            self.table_customer=self.table_customer.join(customer_birtday_transaction,'customer_id',"left").select(self.table_customer.customer_id,
                                            customer_birtday_transaction.birth_date,
                                            self.table_customer.gender,
                                            self.table_customer.location,
                                            self.table_customer.account_balance)
            logging.info(f"Manipulated birth_date column. If birth_date>earliest transaction date birth_date is invalid. chamged it to 1800-01-01 so operational team can rechecj the data to the customer ")
            '''self.table_customer = self.table_customer.withColumn("birth_date", when(self.table_customer.birth_date > "2025-01-01",
                                                                          to_date(concat( day(self.table_customer.birth_date),lit("/"),
                                                                                         month(self.table_customer.birth_date),lit("/"),lit('1800')),"d/M/yyyy")
                                                                        ).otherwise(col("birth_date")))'''
            logging.info(f"===== Finish Transforming {data} data =====")
            return self.table_customer
        
        except Exception as e:
            logging.error(f"====== Failed to Transform Data {data} ======")
            logging.error(e)
    #def convert_time(self,integer): 
        #return time.strftime('%H:%M:%S', time.gmtime(integer))
    
    def transactions (self,data):
        try:
            logging.info(f"===== Start Transforming {data} data =====")
            column = ["TransactionID","CustomerID","TransactionDate","TransactionTime","TransactionAmount (INR)"]
            self.transactions = data.select([data[col] for col in column])
            logging.info(f" Select columns from bank_transaction data")
            RENAME_COLS = {
                "TransactionID": "transaction_id",
                "CustomerID": "customer_id",
                "TransactionDate": "transaction_date",
                "TransactionTime": "transaction_time",
                "TransactionAmount (INR)": "transaction_amount"}
            self.transactions = self.transactions.withColumnsRenamed(colsMap = RENAME_COLS)
            logging.info(f"Renamed columns")
            self.transactions=self.transactions.withColumn('transaction_date', to_date(self.transactions['transaction_date'], "d/M/yy"))
            logging.info(f"Casted transaction_date column to date format")
            self.transactions = self.transactions.withColumn('transaction_time', self.transactions['transaction_time'].cast("int"))
            
            self.transactions = self.transactions.withColumn('transaction_amount', self.transactions['transaction_amount'].cast("float"))
            logging.info(f"Casted transaction_amount column to float")
      
            # Use the map() transformation to apply  
            # the convert_time function to the "transaction_time" column 
            self.transactions = self.transactions.rdd.map(lambda x: (x[0],
                                                                     x[1], 
                                                                     x[2],
                                                                     time.strftime('%H:%M:%S', time.gmtime(x[3])), #to convert integer to time
                                                                     x[4])).toDF(["transaction_id","customer_id","transaction_date","transaction_time","transaction_amount"]) 
            logging.info(f"Casted transaction_time column to time format")
            logging.info(f"===== Finish Transforming {data} data =====")
            return self.transactions
        except Exception as e:
            logging.error(f"====== Failed to Transform Data {data} ======")
            logging.error(e)        
''' def main (self, marketing_campaign_deposit, bank_transaction):
            
'''
    