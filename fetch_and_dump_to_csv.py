from multiprocessing import Pool
import multiprocessing
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
  '''
  receives a timestamp and returns a list of time variables that match the d_timestamp table
  the timestamp is appended to the list as this is used to join d_timestamp on f_songplay
  example:
  row entered = 1541903636796
  returned = [2018, 11, 11, 33, 56, 2, True, 1541903636796]
  '''
 
  new_row = list(datetime.fromtimestamp(int(row // 1000)).timetuple()[0: 7])
  new_row[-1] = new_row[-1] > 5
  new_row.append(row)
  return new_row

def clean_dataframe_of_non_alphanumeric_characters(dataframe, columns):
  '''
  cleans dataframe columns of non alphanumeric or whitespace to ensure they can be entered into DB
  '''
  for col in columns:
    dataframe[col] = dataframe[col].apply(remove_dangerous_characters)

def remove_dangerous_characters(entry):
  '''
  regex that returns all alphanumeric and whitespace characters from a string
  '''
  regex = r'([^\s\w]+)'
  return re.sub(regex, '', str(entry))

def fetch_files_from_s3(suffix):
  '''
  for the log and song directories that are in s3, downloads them to the tmp directory, 
  suffix is the log and song directory directory in s3
  '''
  local_path = os.getcwd() + '/tmp' + f'/{suffix}'
  subprocess.run(f'aws s3 sync s3://udacity-dend/{suffix} {local_path}', shell=True, check=True)

def has_hashable_key(key):
  '''
  function to determine whether a sql joining column has a valid hashable/key that can be used in redshift
  if false the row is dropped from the dataframe
  '''
  return len(str(key)) > 0

def fetch_file_names(file_path, file_type):
  '''
  returns the name of every file in the log and song directories
  list is used in dataframe assignment to aggregate json files into dataframe
  '''

  file_finder = FileFinder(file_path, file_type)
  return file_finder.return_file_names()

def dataframe_assignment(data_dict):
  '''
  parralelize loading log and song directories into dataframe across CPUs
  faster and thread safe as the dataframes don't share memory
  returns log_dataframe, song_dataframe that are assigned using the Parallel lbrary
  '''
  return Parallel(n_jobs=4)(
    delayed(load_dataframe_from_files)(
    list(data_dict[i])) for i in data_dict
  )

def load_dataframe_from_files(file_list):
  '''
  data_loader receives the list of files from the file_finder
  aggregates the files into a list and returns a dataframe from the list for song and log data
  '''
  data_loader = DataLoader(file_list)
  return data_loader.create_dataframe_from_files()


def convert_dataframe_to_csv(dataframe, file_path):
  '''
  takes in a dataframe and file_path
  writes dataframe to file path as zip file
  '''
  dataframe.to_csv(
      path_or_buf=file_path,
      index=False,
      header=False,
      compression='gzip',
      line_terminator='\n'
  )

def fetch_and_dump_to_csv():

  '''
  wrapper function
  downloads the log and song directories from s3
  loads the data into pandas dataframes
  dataframes cleansed to ensure they can be loaded into redshift
  dataframes converted to csvs and stored as gzip for efficiency
  '''
  data_dict = {}
  directories = ['log_data', 'song_data']
  local_path = os.getcwd() + '/tmp'
  aggregate_csv_path = os.getcwd() + '/data/'

  '''
  parralelize fetching files from s3 
  stored in current_dir/tmp/log_data current_dir/tmp/song_data
  '''
  with Pool(processes=multiprocessing.cpu_count()) as pool:
    pool.map(fetch_files_from_s3, directories)
  
  for dir in directories:
    data_dict[dir] = fetch_file_names(f'{local_path}/{dir}/', '*.json')

  '''response from parralelization of loading the contents of directories into dataframes'''
  log_dataframe, song_dataframe = dataframe_assignment(data_dict)

  '''drop rows that can not be used in ETL due to missing data'''
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

  '''
  clean above columns of non-alphanumeric and whitespace characters
  '''
  clean_dataframe_of_non_alphanumeric_characters(song_dataframe, song_text_columns)
  clean_dataframe_of_non_alphanumeric_characters(log_dataframe, log_text_columns)

  dataframe_path_dict = {}
  dataframe_path_dict[aggregate_csv_path + 'song_staging.gz'] = song_dataframe
  dataframe_path_dict[aggregate_csv_path + 'log_staging.gz'] = log_dataframe
  dataframe_path_dict[aggregate_csv_path + 'timestamp_data.gz'] = timestamp_dataframe

  '''
  current_dir/data contains the zip files that are copied to s3 then redshift
  if directory dosen't exist make it
  '''
  if not os.path.exists(aggregate_csv_path):
      os.mkdir(aggregate_csv_path)

  '''
  write each dataframe as gz file to data/path.gz 
  contents of directory synced to s3 then copied to redshift
  '''
  for path in dataframe_path_dict:
    convert_dataframe_to_csv(
      dataframe_path_dict[path], path
    )
  

