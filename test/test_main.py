import unittest
from pyspark.sql import SparkSession
from main import filter_client_country, rename

def test_filter_client_country():
    spark = SparkSession.builder \
        .appName("Test Functions") \
        .getOrCreate()

    # Sample DataFrame for testing
    data = [("John", "USA"), ("Alice", "Canada"), ("Bob", "USA")]
    columns = ["name", "country"]
    df = spark.createDataFrame(data, columns)

    # Test filtering by a single country
    country_name = "USA"
    filtered_df = filter_client_country(country_name, df)
    expected_result = df.filter(df['country'].isin(country_name))
    assert filtered_df.collect() == expected_result.collect()

    # Test filtering by multiple countries
    country_names = ["USA", "Canada"]
    filtered_df_multiple = filter_client_country(country_names, df)
    expected_result_multiple = df.filter(df['country'].isin(country_names))
    assert filtered_df_multiple.collect() == expected_result_multiple.collect()

    spark.stop()

def test_rename():
    spark = SparkSession.builder \
        .appName("Test Functions") \
        .getOrCreate()

    # Sample DataFrame for testing
    data = [("John", "USA"), ("Alice", "Canada"), ("Bob", "USA")]
    columns = ["name", "country"]
    df = spark.createDataFrame(data, columns)

    # Test renaming columns
    rename_dict = {"name": "client_name", "country": "client_country"}
    renamed_df = rename(df, rename_dict)
    expected_columns = ["client_name", "client_country"]
    assert renamed_df.columns == expected_columns

    spark.stop()

if __name__ == '__main__':
    unittest.main()
