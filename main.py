
from pyspark.sql import SparkSession
from argparse import ArgumentParser
import logging


if __name__ == "__main__":
    #Start Logging Configuration
    logging.basicConfig(filename="./logs/log", level=logging.INFO,
                      format='(%(asctime)s):%(levelname)s:%(message)s')

    spark = SparkSession.builder \
        .appName("SparkTest") \
        .getOrCreate()


    from argparse import ArgumentParser

    def get_args():
        '''
        :return: read cmd line input as argument
        '''
        parser = ArgumentParser()
        parser.add_argument("--country", nargs='+', type=str, required=True)  # nargs + means at least one input
        parser.add_argument("--dataset1_path", type=str, required=True, help="Path to dataset one (CSV)")
        parser.add_argument("--dataset2_path", type=str, required=True, help="Path to dataset two (CSV)")
        return parser.parse_args()


    def filter_client_country(country_name, df):
        '''
        :param country_name is the selected country name
        :param df:
        :return: dataframe with
        '''
        df_filtered = df.filter(df['country'].isin(country_name))
        return df_filtered


    def rename(df, rename_dict):
        '''
        :param df: column of original column name, and new column name.
        :return: dataframe with new column name
        '''
        renamed_df = df
        for old_name, new_name in rename_dict.items():
            renamed_df = renamed_df.withColumnRenamed(old_name, new_name)
        return renamed_df


    args = get_args()
    df = spark.read.csv(args.dataset1_path, header=True, inferSchema=True)
    ccinfo = spark.read.csv(args.dataset2_path, header=True, inferSchema=True)

    df_country_filtered = filter_client_country(args.country, df)
    df_country_filtered_NoPI = df_country_filtered.drop("first_name", "last_name")

    ccinfo = ccinfo.drop("cc_n")
    client_data = df_country_filtered_NoPI.join(ccinfo, on='id', how="left" )

    rename_dict = {"id": "client_identifier", "btc_a": "bitcoin_address", "cc_t": "credit_card_type"}
    client_data_rename = rename(client_data, rename_dict)
    client_data_rename.show()
    client_data_rename.write.parquet("client_data")
    spark.stop()
