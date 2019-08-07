from multiprocessing import Pool
import subprocess
import re
from joblib import Parallel, delayed
from lib.file_finder import FileFinder
from lib.data_loader import DataLoader
import os
import pdb
import csv
from datetime import datetime
import pandas as pd

def unpack_timestamp(row):
  new_row = list(datetime.fromtimestamp(int(row // 1000)).timetuple()[0: 7])
  new_row[-1] = new_row[-1] > 5
  return new_row

def clean_dataframe_of_non_alphanumeric_characters(dataframe, columns):
  for col in columns:
    dataframe[col] = dataframe[col].apply(remove_dangerous_characters)

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
    return Parallel(n_jobs=4)(
        delayed(load_dataframe_from_files)(
            list(data_dict[i])) for i in data_dict
    )

def load_dataframe_from_files(file_list):
  data_loader = DataLoader(file_list)
  return data_loader.create_dataframe_from_files()


def convert_dataframe_to_csv(dataframe, file_path):
  dataframe.to_csv(
      path_or_buf=file_path,
      index=False,
      header=False,
      compression='gzip',
      line_terminator='\n'
  )

def fetch_and_dump_to_csv():
  data_dict = {}
  directories = ['log_data', 'song_data']
  local_path = os.getcwd() + '/tmp'
  aggregate_csv_path = os.getcwd() + '/data/'

  #with Pool(processes=multiprocessing.cpu_count()) as pool:
    #pool.map(fetch_files_from_s3, directories)
  
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

  timestamp_dataframe = pd.DataFrame(
      list(map(
          unpack_timestamp, log_dataframe['ts'].values
      ))
  )

  song_text_columns = ['artist_location', 'artist_name', 'title']
  log_text_columns = ['artist', 'location', 'userAgent', 'song']

  clean_dataframe_of_non_alphanumeric_characters(song_dataframe, song_text_columns)
  clean_dataframe_of_non_alphanumeric_characters(log_dataframe, log_text_columns)

  dataframe_path_dict = {}
  dataframe_path_dict[aggregate_csv_path + 'song_staging.gz'] = song_dataframe
  dataframe_path_dict[aggregate_csv_path + 'log_staging.gz'] = log_dataframe
  dataframe_path_dict[aggregate_csv_path + 'timestamp_data.gz'] = timestamp_dataframe

  if not os.path.exists(aggregate_csv_path):
      os.mkdir(aggregate_csv_path)

  for path in dataframe_path_dict:
    convert_dataframe_to_csv(
      dataframe_path_dict[path], path
    )
  


