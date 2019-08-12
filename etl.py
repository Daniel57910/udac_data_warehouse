from lib.database_wrapper import DatabaseWrapper
from fetch_and_dump_to_csv import fetch_and_dump_to_csv
from populate_tables_from_staging import populate_tables_from_staging
import subprocess
from sql.create_tables import table_commands
from sql.queries import select_queries, application_insert_queries
import psycopg2
import os
import configparser
from datetime import datetime
import pandas.io.sql as sqlio
from datetime import datetime

def transfer_from_csv_to_staging(database_wrapper, iam_role):
  '''
  first pushes the staging directory to s3
  then copies each zip file into table using IAM permissions
  '''
  push_staging_files_to_s3(os.getcwd())
  copy_song_staging = "COPY song_staging FROM 's3://sparkify-staging-dmiller/data/song_staging.gz' iam_role '{}' region 'eu-west-2' gzip delimiter ',';".format(iam_role)
  copy_log_staging = "COPY log_staging FROM 's3://sparkify-staging-dmiller/data/log_staging.gz' iam_role '{}' region 'eu-west-2' gzip delimiter ',';".format(iam_role)
  copy_timestamp_staging = "COPY d_timestamp FROM 's3://sparkify-staging-dmiller/data/timestamp_data.gz' iam_role '{}' region 'eu-west-2' gzip delimiter ',';".format(iam_role)
  database_wrapper.execute(copy_song_staging)
  database_wrapper.execute(copy_log_staging)
  database_wrapper.execute(copy_timestamp_staging)

def push_staging_files_to_s3(current_dir):
  '''
  bash subprocess to sync the directory containing the staging files to s3
  '''
  subprocess.run(f'aws s3 sync {current_dir}/data s3://sparkify-staging-dmiller/data/',shell=True,check=True)

def main():

  config = configparser.ConfigParser()
  config.read('dwh.cfg')

  '''
  create redshift connection string from configuration file to connect to db
  '''
  conn_string = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())

  '''
  class used to manage databse transactions
  '''
  database_wrapper = DatabaseWrapper(conn_string)

  '''
  IAM Role to manage transactions between Redshift & s3
  '''
  iam_role = config['IAM_ROLE']['IAM_ROLE']

  '''
  first drop every table
  then create every table in create_tables.table_commands list
  '''
  for command in table_commands:
    database_wrapper.execute(command)

  fetch_and_dump_to_csv()

  app_users = []

  '''
  files generated from fetch_and_dump_from_csv synched to s3
  '''
  transfer_from_csv_to_staging(database_wrapper, iam_role)

  '''
  files copied from s3 to staging and d_timestamp tables
  copied directly as gzip files
  '''
  populate_tables_from_staging(database_wrapper)

  '''
  as redshift does not support upsert
  distinct app user ids selected from d_app_user_id
  '''
  distinct_app_users = database_wrapper.select(select_queries['distinct_app_user'])

  '''
  then the rest of the data required for d_app_user identified
  fetched by finding all that match app_user_id and selecting most recent by timestamp
  as most recent songplay contains correct information on app_user
  '''
  for user in distinct_app_users:
    result = database_wrapper.select_all(
      select_queries['ordered_app_user'].format(user) 
    )
    app_users.extend(result)

  '''
  app_users inserted into d_app_user table
  '''
  database_wrapper.execute_batch(
    application_insert_queries['insert_app_user'],
    app_users
  )

  '''
  data from log_staging to populate fact table selected 
  loaded into dataframe for reinsertion into f_songplay
  '''
  song_play_dataframe = sqlio.read_sql(
    select_queries['songplay_fetch'],
    database_wrapper.conn
  )

  '''
  songplay dataframe converted into list of tuples, each column an item in the tuple
  data from tuple joined on d_artist, d_song, d_app_user and inserted into f_songplay
  '''
  database_wrapper.execute_batch(
    application_insert_queries['songplay_insert'], 
    list(
      song_play_dataframe.itertuples(index=False, name=None)
    )
  )

  
  


if __name__ == "__main__":
    main()

