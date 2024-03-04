
from pyspark.sql import SparkSession
from argparse import ArgumentParser
import logging
import utils
if __name__ == "__main__":
    #Start Spark Session
    spark = SparkSession.builder \
        .appName("SparkTest") \
        .getOrCreate()
    #Parse the arguement into object
    args = utils.get_args()  #
    #Reading in the csv file
    df = spark.read.csv(args.dataset1_path, header=True, inferSchema=True)
    ccinfo = spark.read.csv(args.dataset2_path, header=True, inferSchema=True)
    #Filter country with the intake argument
    df_UKNL = utils.filter_client_country(args.country, df)
    #Keep email and drop other PII
    df_UKNL_NoPI = df_UKNL.drop("first_name", "last_name")
    #Drop credit card number
    ccinfo = ccinfo.drop("cc_n")
    #Join all table together
    client_data = df_UKNL_NoPI.join(ccinfo, on='id', how="left" )
    #Rename the id,btc_a, cc_t field
    rename_dict = {"id": "client_identifier", "btc_a": "bitcoin_address", "cc_t": "credit_card_type"}
    client_data_rename = utils.rename(client_data, rename_dict)
    #Start Logging Configuration
    logging.basicConfig(filename="./logs/log", level=logging.INFO,
                      format='(%(asctime)s):%(levelname)s:%(message)s')
    # Adjust the outfile path here
    client_data_rename.write.parquet("client_data")
    #Stopping sparks
    spark.stop()