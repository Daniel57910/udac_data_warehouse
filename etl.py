from lib.database_wrapper import DatabaseWrapper
from fetch_and_dump_to_csv import fetch_and_dump_to_csv
from populate_tables_from_staging import populate_tables_from_staging
import subprocess
from sql.create_tables import table_commands
from sql.queries import select_queries, application_insert_queries
import psycopg2
import pdb
import os
import configparser
from datetime import datetime
import pandas.io.sql as sqlio
from datetime import datetime

def transfer_from_csv_to_staging(database_wrapper, iam_role):
  push_staging_files_to_s3(os.getcwd())
  copy_song_staging = "COPY song_staging FROM 's3://sparkify-staging-dmiller/data/song_staging.gz' iam_role '{}' region 'eu-west-2' gzip delimiter ',';".format(iam_role)
  copy_log_staging = "COPY log_staging FROM 's3://sparkify-staging-dmiller/data/log_staging.gz' iam_role '{iam_role}' region 'eu-west-2' gzip delimiter ',';".format(iam_role)
  copy_timestamp_staging = "COPY d_timestamp FROM 's3://sparkify-staging-dmiller/data/timestamp_data.gz' iam_role '{iam_role}' region 'eu-west-2' gzip delimiter ',';".format(iam_role)
  database_wrapper.execute(copy_song_staging)
  database_wrapper.execute(copy_log_staging)
  database_wrapper.execute(copy_timestamp_staging)

def push_staging_files_to_s3(current_dir):
  subprocess.run(f'aws s3 sync {current_dir}/data s3://sparkify-staging-dmiller/data/',shell=True,check=True)

def main():

  start = datetime.now()
  config = configparser.ConfigParser()
  config.read('dwh.cfg')
  conn_string = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
  database_wrapper = DatabaseWrapper(conn_string)
  iam_role = config['IAM_ROLE']['IAM_ROLE']

  for command in table_commands:
    database_wrapper.execute(command)

  app_users = []
  fetch_and_dump_to_csv()
  transfer_from_csv_to_staging(database_wrapper)
  populate_tables_from_staging(database_wrapper, iam_role)

  distinct_app_users = database_wrapper.select(select_queries['distinct_app_user'])

  for user in distinct_app_users:
    result = database_wrapper.select_all(
      select_queries['ordered_app_user'].format(user[0]) 
    )
    app_users.append(result)

  database_wrapper.execute_batch(
    application_insert_queries['insert_app_user'],
    app_users
  )

  song_play_dataframe = sqlio.read_sql(
    select_queries['songplay_fetch'],
    database_wrapper.conn
  )

  database_wrapper.execute_batch(
    application_insert_queries['songplay_insert'], 
    list(
      song_play_dataframe.itertuples(index=False, name=None)
    )
  )

  print('Time taken to execute = {}'.format(datetime.now() - start))

  
  


if __name__ == "__main__":
    main()

