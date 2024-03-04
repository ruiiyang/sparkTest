from argparse import ArgumentParser
def get_args():
    '''
    :return: read cmd line input as argument
    '''
    parser = ArgumentParser()
    parser.add_argument("--country", nargs='+', type=str, required=True) #nargs + means at least one input
    parser.add_argument("--dataset1_path", type=str, required=True, help="Path to dataset one (CSV)")
    parser.add_argument("--dataset2_path", type=str, required=True, help="Path to dataset two (CSV)")
    return parser.parse_args()


def filter_client_country (country_name, df):
    '''
    :param country_name is the selected country name
    :param df:
    :return: dataframe with
    '''
    df_filtered = df.filter(df['country'].isin(country_name))
    return df_filtered


def rename (df,rename_dict):
    '''
    :param df: column of original column name, and new column name.
    :return: dataframe with new column name
    '''
    renamed_df = df
    for old_name, new_name in rename_dict.items():
        renamed_df = renamed_df.withColumnRenamed(old_name, new_name)
    return renamed_df
