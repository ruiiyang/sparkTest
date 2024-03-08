
from pyspark.sql import SparkSession
from argparse import ArgumentParser
import logging
import utils

if __name__ == "__main__":
    #Start Logging Configuration
    logging.basicConfig(filename="./logs/log", level=logging.INFO,
                      format='(%(asctime)s):%(levelname)s:%(message)s')

    spark = SparkSession.builder \
        .appName("SparkTest") \
        .getOrCreate()

    args = utils.get_args()  #

    df = spark.read.csv(args.dataset1_path, header=True, inferSchema=True)
    ccinfo = spark.read.csv(args.dataset2_path, header=True, inferSchema=True)

    df_country_filtered = utils.filter_client_country(args.country, df)
    df_country_filtered_NoPI = df_country_filtered.drop("first_name", "last_name")

    ccinfo = ccinfo.drop("cc_n")
    client_data = df_country_filtered_NoPI.join(ccinfo, on='id', how="left" )

    rename_dict = {"id": "client_identifier", "btc_a": "bitcoin_address", "cc_t": "credit_card_type"}
    client_data_rename = utils.rename(client_data, rename_dict)

    client_data_rename.write.parquet("client_data")
    spark.stop()