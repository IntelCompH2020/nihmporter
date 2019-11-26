import pathlib
from typing import List, Union

import pandas as pd
import yaml


def dataframe_from_csv_files(csv_files: List[Union[str, pathlib.Path]], dtype=None) -> pd.DataFrame:

	# a list with a `DataFrame` for every csv file
	per_year_dfs = []

	# for every csv file in the unzipped files directory...
	for csv_file in csv_files:

		# ...the file is read into a `DataFrame` and added to the above list
		per_year_dfs.append(pd.read_csv(csv_file, encoding='iso8859_15', dtype=dtype))

	# all the `DataFrame`s are concatenated together
	df = pd.concat(per_year_dfs, axis=0)

	# # some curating
	# df['LANG'] = df['LANG'].astype('category')
	# df['PUB_YEAR'] = pd.to_datetime(df['PUB_YEAR'], format='%Y')

	# in order to save the `DataFrame` in a feather file, we need to reset the index
	df.reset_index(inplace=True)

	# the old index was artificial and not actually needed
	df.drop(['index'], axis=1, inplace=True)

	return df


def dataframe_types_to_yaml(df: pd.DataFrame) -> str:
	"""
	Convenience function to extract the (inferred) `dtypes` from a `DataFrame`.

	Parameters
	----------
	df: dataframe
		The input dataframe.

	Returns
	-------
	out: str
		A string ready to be included in a yaml file.

	"""

	return yaml.dump(df.dtypes.apply(lambda x: x.name).to_dict())
