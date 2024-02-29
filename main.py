
from pyspark.sql import SparkSession
import logging
#from utils import
# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read CSV Example") \
    .getOrCreate()
#Reading in the csv file
df = spark.read.csv ('dataset_one.csv', header=True, inferSchema=True)
ccinfo = spark.read.csv ('dataset_two.csv', header=True, inferSchema=True)
#Filter country
df_UKNL = df.filter(df['country'].isin('United Kingdom', 'the Netherlands'))
#Keep email and drop other PII
df_UKNL_NoPI = df_UKNL.drop("first_name","last_name")
#Drop credit card number
ccinfo = ccinfo.drop("cc_n")
#Join all table together
client_data = df_UKNL_NoPI.join(ccinfo, on='id', how="left" )
client_data.show()
#Rename the id,btc_a, cc_t field
client_data_rename = client_data.withColumnRenamed("id", "client_identifier")\
    .withColumnRenamed("btc_a", "bitcoin_address")\
    .withColumnRenamed("cc_t", "credit_card_type")
client_data_rename.show()
# Adjust the file path here
output_path = "./client_data"
client_data_rename.write.parquet(output_path)

#stopping sparks
spark.stop()


#if __name__ == "__main__":