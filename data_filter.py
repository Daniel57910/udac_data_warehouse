
class DataFilter:

	def __init__(self, panda_dataframe):
		self.panda_dataframe = panda_dataframe

	def return_unique_dataframe_subset(self, columns, dup_index=None):

		if not dup_index: return self.panda_dataframe[columns]
		return self.panda_dataframe[columns].drop_duplicates(subset=dup_index, keep='last')
		

	