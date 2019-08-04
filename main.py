from lib.database_wrapper import DatabaseWrapper
from fetch_and_dump_to_csv import fetch_and_dump_to_csv
import subprocess
from sql.tables import table_commands
import psycopg2
import pdb
import os
import configparser


def push_staging_files_to_s3(current_dir):
  subprocess.run(f'aws s3 sync {current_dir}/data s3://sparkify-staging-dmiller/data/',shell=True,check=True)

def main():

  config = configparser.ConfigParser()
  config.read('secrets.ini')
  conn_string = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
  database_wrapper = DatabaseWrapper(conn_string)

  for command in table_commands:
    database_wrapper.execute(command)

  fetch_and_dump_to_csv()
  # push_staging_files_to_s3(os.getcwd())

  copy_song_staging = "COPY song_staging FROM 's3://sparkify-staging-dmiller/data/song_staging.csv' iam_role 'arn:aws:iam::774141665752:role/redshift_s3_role' region 'eu-west-2' delimiter ',';"
  copy_log_staging = "COPY log_staging FROM 's3://sparkify-staging-dmiller/data/log_staging.csv' iam_role 'arn:aws:iam::774141665752:role/redshift_s3_role' region 'eu-west-2' delimiter ',';"

  # database_wrapper.execute(copy_song_staging)
  # database_wrapper.execute(copy_log_staging)


if __name__ == "__main__":
    main()

