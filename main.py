import multiprocessing
from multiprocessing import Pool
from lib.file_finder import FileFinder
import subprocess

def fetch_files_from_s3(suffix):
  subprocess.run(f'aws s3 sync s3://udacity-dend/{suffix} /tmp/{suffix}', shell=True, check=True)

def fetch_file_names(suffix, file_type):
  file_finder = FileFinder(f'/tmp/{suffix}/', file_type)
  return file_finder.return_file_names()

def main():

  data_dict = {}
  directories = ['log_data', 'song_data']
  # with Pool(processes=multiprocessing.cpu_count()) as pool:
  #   [pool.apply_async(fetch_files_from_s3, (dir, )) for dir in directories]

  data_dict['log_data'] = fetch_file_names(directories[0], '*.json')

  for d in data_dict['log_data']:
    print(d)

  

if __name__ == "__main__":
    main()

