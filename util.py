import pathlib
from typing import List, Union

import pandas as pd


def dataframe_from_csv_files(csv_files: List[Union[str, pathlib.Path]]) -> pd.DataFrame:

	# a list with a `DataFrame` for every csv file
	per_year_dfs = []

	# for every csv file in the unzipped files directory...
	for csv_file in csv_files:
		# ...the file is read into a `DataFrame` and added to the above list
		per_year_dfs.append(pd.read_csv(csv_file, encoding='iso8859_15'))

	# all the `DataFrame`s are concatenated together
	df = pd.concat(per_year_dfs, axis=0)

	# some curating
	df['LANG'] = df['LANG'].astype('category')
	df['PUB_YEAR'] = pd.to_datetime(df['PUB_YEAR'], format='%Y')

	# in order to save the `DataFrame` in a feather file, we need to reset the index
	df.reset_index(inplace=True)

	# the old index was artificial and not actually needed
	df.drop(['index'], axis=1, inplace=True)

	return df