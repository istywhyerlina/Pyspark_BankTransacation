from pyspark.sql import SparkSession


# Inisialisasi SparkSession
spark = SparkSession.builder.appName("TransformDate").getOrCreate()

# handle legacy time parser
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Extract data 

# Transform data

# Load data
