from lib.database_wrapper import DatabaseWrapper
from fetch_and_dump_to_csv import fetch_and_dump_to_csv
from populate_tables_from_staging import populate_tables_from_staging
import subprocess
from sql.tables import table_commands
import psycopg2
import pdb
import os
import configparser

def transfer_from_csv_to_staging(database_wrapper):
  # push_staging_files_to_s3(os.getcwd())
  copy_song_staging = "COPY song_staging FROM 's3://sparkify-staging-dmiller/data/song_staging.csv' iam_role 'arn:aws:iam::774141665752:role/redshift_s3_role' region 'eu-west-2' delimiter ',';"
  copy_log_staging = "COPY log_staging FROM 's3://sparkify-staging-dmiller/data/log_staging.csv' iam_role 'arn:aws:iam::774141665752:role/redshift_s3_role' region 'eu-west-2' delimiter ',';"
  database_wrapper.execute(copy_song_staging)
  database_wrapper.execute(copy_log_staging)


def push_staging_files_to_s3(current_dir):
  subprocess.run(f'aws s3 sync {current_dir}/data s3://sparkify-staging-dmiller/data/',shell=True,check=True)

def main():

  distinct_app_user_query = '''SELECT DISTINCT * from d_app_user_id;'''
  ordered_app_user_query = '''SELECT TOP 1 * FROM d_app_user_staging
  WHERE
    app_user_id = {}
  ORDER BY timestamp desc
  ;'''
  config = configparser.ConfigParser()
  config.read('secrets.ini')
  conn_string = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
  database_wrapper = DatabaseWrapper(conn_string)

  # for command in table_commands:
  #   database_wrapper.execute(command)

  users = []
  # fetch_and_dump_to_csv()
  # transfer_from_csv_to_staging(database_wrapper)
  # populate_tables_from_staging(database_wrapper)

  distinct_app_users = database_wrapper.select(distinct_app_user_query)

  print('executing query for distinct_app_users')
  for user in distinct_app_users:
    result = database_wrapper.select(ordered_app_user_query.format(user[0]), True)
    users.append(result)
    print(result)

  for u in users:
    print(u)



if __name__ == "__main__":
    main()

