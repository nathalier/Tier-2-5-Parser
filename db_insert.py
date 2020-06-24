from glob import glob
import pandas as pd
import sys
from sqlalchemy import create_engine
from connection_settings import params

def freshest_data():
    # find all sponsors file and return the path to the freshest one
    files = glob('./sponsors/tier*.csv')
    files = sorted(files)
    return files[-1]


def insert_to_db(sponsors_file):
    conn_params = f"postgresql://{params['user']}:{params['password']}@{params['host']}/{params['database']}"
    engine = create_engine(conn_params)

    # insert tier types into db
    df_tier_types = pd.read_csv('tier_types.csv')
    df_tier_types.index.name = 'tier_type_id'
    df_tier_types.to_sql(name='tier_types', con=engine, if_exists='replace', method='multi')

    df_sponsors = pd.read_csv(sponsors_file)

    # insert sponsors into db
    df_sponsors_unique = df_sponsors[['name', 'city', 'county']].drop_duplicates().reset_index(drop=True)
    df_sponsors_unique.index.name = 'sponsor_id'
    df_sponsors_unique.to_sql(name='sponsors', con=engine, if_exists='replace', method='multi')

    # fill sponsor-visa_type table (many to many relation)
    df_sponsors_unique['sponsor_id'] = df_sponsors_unique.index
    df_sponsors_with_id = pd.merge(df_sponsors, df_sponsors_unique, on=['name', 'city', 'county'])
    df_tier_types['tier_type_id'] = df_tier_types.index
    df_sponsors_visas = pd.merge(df_sponsors_with_id, df_tier_types, on=['tier_type', 'tier_subtype'])
    df_sponsors_visas[['sponsor_id', 'tier_type_id', 'tier_rating']].\
        to_sql(name='sponsors_visas', con=engine, if_exists='replace', method='multi')

    print(f'Successfully inserted from {sponsors_file}')

if __name__ == '__main__':
    if len(sys.argv) == 2:
        insert_to_db(sys.argv[1])
    else:
        insert_to_db(freshest_data())
