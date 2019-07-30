import pandas as pd
import json
import pdb
import re
class DataLoader:

	def __init__(self, matched_files):
		self.matched_files = matched_files

	def create_dataframe_from_files(self):

		aggregate_file_data = []

		for file in self.matched_files:
			aggregate_file_data.extend(fetch_json_data(file))
		
		return pd.DataFrame.from_records(aggregate_file_data)

def fetch_json_data(filename):
	
    json_file_data = []
    with open(filename, 'r') as f:
      for line in f:
        try:
          json_file_data.append(json.loads(line))
        except Exception as e:
          print('Unable to load line: line contents = {}'.format(line))
          print(e)

    return json_file_data

