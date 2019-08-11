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
import pandas.io.sql as sqlio


def transfer_from_csv_to_staging(database_wrapper):
  push_staging_files_to_s3(os.getcwd())
  copy_song_staging = "COPY song_staging FROM 's3://sparkify-staging-dmiller/data/song_staging.gz' iam_role 'arn:aws:iam::774141665752:role/redshift_s3_role' region 'eu-west-2' gzip delimiter ',';"
  copy_log_staging = "COPY log_staging FROM 's3://sparkify-staging-dmiller/data/log_staging.gz' iam_role 'arn:aws:iam::774141665752:role/redshift_s3_role' region 'eu-west-2' gzip delimiter ',';"
  copy_timestamp_staging = "COPY d_timestamp FROM 's3://sparkify-staging-dmiller/data/timestamp_data.gz' iam_role 'arn:aws:iam::774141665752:role/redshift_s3_role' region 'eu-west-2' gzip delimiter ',';"
  database_wrapper.execute(copy_song_staging)
  database_wrapper.execute(copy_log_staging)
  database_wrapper.execute(copy_timestamp_staging)

def push_staging_files_to_s3(current_dir):
  subprocess.run(f'aws s3 sync {current_dir}/data s3://sparkify-staging-dmiller/data/',shell=True,check=True)

def main():

  distinct_app_user_query = '''SELECT DISTINCT * from d_app_user_id;'''
  ordered_app_user_query = ''' SELECT TOP 1 app_user_id, first_name, last_name, gender, level
   FROM d_app_user_staging
  WHERE
    app_user_id = {}
  ORDER BY timestamp desc
  ;'''

  songplay_staging_fetch_all = '''SELECT first_name, last_name, level, artist, song_name, artist, session_id, location, user_agent, timestamp FROM log_staging;'''

  songplay_insert = '''INSERT INTO f_songplay 
    (user_id, level, artist_key, song_key, session_id, location, user_agent, start_time) VALUES (
    (select top 1 app_user_key from d_app_user where first_name = %s and last_name = %s order by app_user_id DESC),
    %s,
    (select top 1 artist_key from d_artist where artist_name = %s),
    (select song_key from d_song where title = %s and artist_id = (SELECT top 1 artist_id from d_artist where artist_name = %s)),
    %s,
    %s,
    %s,
    %s
    );'''

  insert_app_user_query = ''' INSERT INTO d_app_user (app_user_id, first_name, last_name, gender, level) VALUES {};'''
  insert_timestamp_query ='''INSERT INTO d_timestamp (year, month, day, hour, minute, second, weekday) VALUES {};'''
  timestamp_query ='''SELECT timestamp from log_staging''';
  
  config = configparser.ConfigParser()
  config.read('secrets.ini')
  conn_string = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
  database_wrapper = DatabaseWrapper(conn_string)

  # for command in table_commands:
  #   database_wrapper.execute(command)

  # users = []
  # fetch_and_dump_to_csv()
  # transfer_from_csv_to_staging(database_wrapper)
  # populate_tables_from_staging(database_wrapper)

  # distinct_app_users = database_wrapper.select(distinct_app_user_query)

  # for user in distinct_app_users:
  #   result = database_wrapper.select_all(
  #     ordered_app_user_query.format(user[0]) 
  #   )
  #   users.append(result)

  # for user in users:
  #   database_wrapper.execute(
  #     insert_app_user_query.format(user)
  #   )

  song_play_dataframe = sqlio.read_sql(
    songplay_staging_fetch_all,
    database_wrapper.conn
  )

  

  song_play_dataframe = list(song_play_dataframe.itertuples(index=False, name=None))
  print(len(song_play_dataframe))

  database_wrapper.execute_batch(
    songplay_insert, song_play_dataframe
  )

  
  


if __name__ == "__main__":
    main()

