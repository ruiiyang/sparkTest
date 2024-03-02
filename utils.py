from argparse import ArgumentParser
def get_args():
    parser = ArgumentParser()
    parser.add_argument("--country",nargs='+', type=str, required=True)
    return parser.parse_args()


def filter_client_country (args,df):
    args = get_args()
    country_name = args
    print(country_name)
    df_filtered = df.filter(df['country'].isin(country_name))
    return df_filtered


#def rename_payment_detail:


def get_args():
    parser = ArgumentParser()