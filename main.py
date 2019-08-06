from lib.database_wrapper import DatabaseWrapper
from fetch_and_dump_to_csv import fetch_and_dump_to_csv
from populate_tables_from_staging import populate_tables_from_staging
import subprocess
from sql.tables import table_commands
import psycopg2
import pdb
import os
import configparser
from datetime import datetime
import csv

def transfer_from_csv_to_staging(database_wrapper):
  # push_staging_files_to_s3(os.getcwd())
  copy_song_staging = "COPY song_staging FROM 's3://sparkify-staging-dmiller/data/song_staging.csv' iam_role 'arn:aws:iam::774141665752:role/redshift_s3_role' region 'eu-west-2' delimiter ',';"
  copy_log_staging = "COPY log_staging FROM 's3://sparkify-staging-dmiller/data/log_staging.csv' iam_role 'arn:aws:iam::774141665752:role/redshift_s3_role' region 'eu-west-2' delimiter ',';"
  database_wrapper.execute(copy_song_staging)
  database_wrapper.execute(copy_log_staging)

def push_staging_files_to_s3(current_dir):
  subprocess.run(f'aws s3 sync {current_dir}/data s3://sparkify-staging-dmiller/data/',shell=True,check=True)

def unpack_timestamp(row):
  new_row = list(datetime.fromtimestamp(int(row[0] // 1000)).timetuple()[0: 7])
  new_row[-1] = new_row[-1] > 5
  return new_row

def main():

  distinct_app_user_query = '''SELECT DISTINCT * from d_app_user_id;'''
  ordered_app_user_query = ''' SELECT TOP 1 app_user_id, first_name, last_name, gender, level
   FROM d_app_user_staging
  WHERE
    app_user_id = {}
  ORDER BY timestamp desc
  ;'''

  insert_app_user_query = ''' INSERT INTO d_app_user (app_user_id, first_name, last_name, gender, level) VALUES {};'''
  insert_timestamp_query ='''INSERT INTO d_timestamp (year, month, day, hour, minute, second, weekday) VALUES {};'''
  timestamp_query ='''SELECT timestamp from log_staging''';
  
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

  # distinct_app_users = database_wrapper.select(distinct_app_user_query)

  # for user in distinct_app_users:
  #   result = database_wrapper.select(
  #     ordered_app_user_query.format(user[0]), 
  #     True
  #   )
  #   database_wrapper.execute(
  #     insert_app_user_query.format(result)
  #   )

  timestamp_results = database_wrapper.select(timestamp_query)
  timestamp_results = list(map(unpack_timestamp, timestamp_results))

  timestamp_path = os.getcwd() + '/data/timestamp.csv'
  with open(timestamp_path, 'w') as timestamp_file:
    writer = csv.writer(timestamp_file)
    writer.writerows(list(
      map(unpack_timestamp, timestamp_results))
    )

if __name__ == "__main__":
    main()

