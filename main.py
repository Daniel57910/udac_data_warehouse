import multiprocessing
from multiprocessing import Pool
from lib.file_finder import FileFinder
from lib.data_loader import DataLoader
from lib.database_wrapper import DatabaseWrapper
import subprocess
import os
from joblib import Parallel, delayed
from sql.tables import table_commands
import psycopg2
import configparser
import pdb
import re

def remove_dangerous_characters(entry):
  regex = r'([^\s\w]+)'
  return re.sub(regex, '', str(entry))


def fetch_files_from_s3(suffix):
  local_path = os.getcwd() + '/tmp' + f'/{suffix}'
  subprocess.run(f'aws s3 sync s3://udacity-dend/{suffix} {local_path}', shell=True, check=True)

def has_hashable_key(key):
  return len(str(key)) > 0

def fetch_file_names(file_path, file_type):
  file_finder = FileFinder(file_path, file_type)
  return file_finder.return_file_names()

def dataframe_assignment(data_dict):

    # first_files = data_dict['song_data']

    # load_dataframe_from_files(first_files)
    return Parallel(n_jobs=4)(
      delayed(load_dataframe_from_files)(list(data_dict[i])) for i in data_dict
    )

def load_dataframe_from_files(file_list):
  data_loader = DataLoader(file_list)
  return data_loader.create_dataframe_from_files()

def push_staging_files_to_s3(current_dir):
  subprocess.run(f'aws s3 sync {current_dir}/data s3://sparkify-staging-dmiller/data/',shell=True,check=True)

def main():

  data_dict = {}
  directories = ['log_data', 'song_data']
  local_path = os.getcwd() + '/tmp'
  aggregate_csv_path = os.getcwd() + '/data/'
  config = configparser.ConfigParser()
  config.read('secrets.ini')
  conn_string = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
  song_path = os.getcwd() + '/song_data.csv'

  database_wrapper = DatabaseWrapper(conn_string)

  for command in table_commands:
    database_wrapper.execute(command)

  # with Pool(processes=multiprocessing.cpu_count()) as pool:
  #   pool.map(fetch_files_from_s3, directories)

  for dir in directories:
    data_dict[dir] = fetch_file_names(f'{local_path}/{dir}/', '*.json')

  log_dataframe, song_dataframe = dataframe_assignment(data_dict)

  song_dataframe = song_dataframe[
    song_dataframe.artist_id.apply(has_hashable_key) &
    song_dataframe.song_id.apply(has_hashable_key) &
    song_dataframe.artist_name.apply(has_hashable_key) &
    song_dataframe.title.apply(has_hashable_key) 
  ]

  log_dataframe = log_dataframe[
      log_dataframe.userId.apply(has_hashable_key) &
      log_dataframe.firstName.apply(has_hashable_key) &
      log_dataframe.lastName.apply(has_hashable_key)
  ]

  song_text_columns = ['artist_location', 'artist_name', 'title']
  log_text_columns =  ['artist', 'location', 'userAgent', 'song']

  clean_dataframe_of_non_alphanumeric_characters(song_dataframe, song_text_columns)
  clean_dataframe_of_non_alphanumeric_characters(log_dataframe, log_text_columns)

  song_dataframe.to_csv(
    path_or_buf=aggregate_csv_path + 'song_staging.csv', 
    index=False,
    header=False
  )

  log_dataframe.to_csv(
    path_or_buf=aggregate_csv_path + 'log_staging.csv',
    sep=',',
    index=False,
    header=False
  )

  push_staging_files_to_s3(os.getcwd())

  copy_song_staging = "COPY song_staging FROM 's3://sparkify-staging-dmiller/data/song_staging.csv' iam_role 'arn:aws:iam::774141665752:role/redshift_s3_role' region 'eu-west-2' delimiter ',';"
  copy_log_staging = "COPY log_staging FROM 's3://sparkify-staging-dmiller/data/log_staging.csv' iam_role 'arn:aws:iam::774141665752:role/redshift_s3_role' region 'eu-west-2' delimiter ',';"

  database_wrapper.execute(copy_song_staging)
  database_wrapper.execute(copy_log_staging)

def clean_dataframe_of_non_alphanumeric_characters(dataframe, columns):
  for col in columns:
    dataframe[col] =  dataframe[col].apply(remove_dangerous_characters)
  


if __name__ == "__main__":
    main()

