import multiprocessing
from multiprocessing import Pool
from lib.file_finder import FileFinder
from lib.data_loader import DataLoader
import subprocess
import os
from joblib import Parallel, delayed

def fetch_files_from_s3(suffix):
  subprocess.run(f'aws s3 sync s3://udacity-dend/{suffix} /tmp/{suffix}', shell=True, check=True)

def has_hashable_key(key):
  return len(str(key)) > 0

def fetch_file_names(suffix, file_type):
  file_finder = FileFinder(f'/tmp/{suffix}/', file_type)
  return file_finder.return_file_names()

def dataframe_assignment(data_dict):
    return Parallel(n_jobs=4)(
      delayed(load_dataframe_from_files)(list(data_dict[i])) for i in data_dict
    )

def load_dataframe_from_files(file_list):
  data_loader = DataLoader(file_list)
  return data_loader.create_dataframe_from_files()

def main():

  data_dict = {}
  directories = ['log_data', 'song_data']

  # with Pool(processes=multiprocessing.cpu_count()) as pool:
  #   [pool.apply_async(fetch_files_from_s3, (dir, )) for dir in directories]

  for dir in directories:
    data_dict[dir] = fetch_file_names(dir, '*.json')

  log_dataframe, song_dataframe = dataframe_assignment(data_dict)

  song_dataframe = song_dataframe[
    song_dataframe.artist_id.apply(has_hashable_key) &
    song_dataframe.song_id.apply(has_hashable_key) &
    song_dataframe.artist_name.apply(has_hashable_key) &
    song_dataframe.title.apply(has_hashable_key)
  ]

  song_dataframe.to_csv(
    path_or_buf=os.getcwd()+'/song_data.csv', index=False
  )








  

if __name__ == "__main__":
    main()
