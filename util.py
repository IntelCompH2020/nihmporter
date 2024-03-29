import pathlib
from typing import List, Union, Optional

import pandas as pd
import yaml


def dataframe_from_csv_files(
		csv_files: List[Union[str, pathlib.Path]], dtype: Optional[dict] = None, source_column: str = 'source_file'
) -> pd.DataFrame:
	"""
	Reads a collection of CSV files with the same structure into a `DataFrame`.

	Parameters
	----------
	csv_files: list
		The paths to the CSV files.
	dtype: dict, optional
		Specifies the type of every column/field found in (all) the CSV files.
	source_column: str
		The name of the new (metadata) column indicating the source file.

	Returns
	-------
	out: `DataFrame`
		Data in all the CSV files (vertically) concatenated.

	"""

	# a list with a `DataFrame` for every csv file
	dfs_list = []

	# for every csv file in the unzipped files subdirectory...
	for csv_file in csv_files:

		# csv_file_df = pd.read_csv(csv_file, encoding='iso8859_15', dtype=dtype, error_bad_lines=False)
		csv_file_df = pd.read_csv(csv_file, encoding='iso8859_15', dtype=dtype, on_bad_lines='warn')

		# a new (metadata) column is added to indicate the "source" file
		csv_file_df[source_column] = csv_file.name

		# the new `DataFrame` is added to the above list
		dfs_list.append(csv_file_df)

	# all the `DataFrame`s are (vertically) concatenated; they need *not* have the exact same columns, but (by default)
	# an "outer" join is performed, and the columns are sorted
	df = pd.concat(dfs_list, axis=0, sort=True)

	# in order to save the `DataFrame` in a feather file, we need to reset the index
	df.reset_index(inplace=True)

	# the old index was artificial and not actually needed
	df.drop(['index'], axis=1, inplace=True)

	# for the sake of efficiency
	df[source_column] = df[source_column].astype('category')

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


def modification_date_from_path(f: Union[str, pathlib.Path]) -> pd.Timestamp:

	# in case a `str` was passed
	f = pathlib.Path(f)

	return pd.Timestamp.fromtimestamp(f.stat().st_mtime)
