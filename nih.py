import pathlib
import re
import urllib.parse
from typing import List, Optional

import numpy as np
import pandas as pd
import bs4 as bs
import requests

import download
import util


class DataBunch:

	def __init__(self, parameters: dict) -> None:

		self.parameters = parameters

		self.nih_homepage = parameters['homepage']
		self.name = parameters['name']
		self.downloads_directory = pathlib.Path(parameters['output']['downloads directory'])
		self.unzipped_files_directory = pathlib.Path(parameters['output']['unzipped files directory'])
		self.feather_file = pathlib.Path(parameters['feather file'])
		self.key_columns = parameters['key columns']

		# the URL to the appropriate subpage within the NIH homepage
		self.url = urllib.parse.urljoin(parameters['homepage'], parameters['relative path'])

		# regular expressions defining the files that must be ignored
		self.re_files_to_be_ignored = [re.compile(expr) for expr in parameters['filename patterns to be ignored']]

		# to act as cache
		self.df: Optional[pd.DataFrame] = None

	def parse_html_table(self) -> pd.DataFrame:
		"""
		Parses the appropriate HTML table into a `DataFrame`.

		Returns
		-------
		out: dataframe
			The requested table.

		"""

		page = requests.get(self.url)

		# =================================== parsing

		# the web is parsed...
		sp = bs.BeautifulSoup(page.content, 'lxml')

		# ...and *all* the tables extracted
		tables = sp.find_all('table')

		table_of_interest = tables[self.parameters['table number']]

		# the table of interest is further parsed into a Pandas `DataFrame`
		df = pd.read_html(str(table_of_interest), encoding='utf-8', header=0)[0]

		# a numpy array with either links or "no link"s
		links = [np.where(tag.has_attr('href'), tag.get('href'), 'no link') for tag in table_of_interest.find_all('a')]

		# it is turned into a regular Python list and stored as an instance attribute for later use
		links = [l.item() for l in links]

		for pattern in self.re_files_to_be_ignored:

			links = [l for l in links if not pattern.match(l)]

		return self._merge_links(df, links)

	@staticmethod
	def _merge_links(df: pd.DataFrame, links: List[str]) -> pd.DataFrame:
		"""
		Add columns for the links to the `DataFrame`.

		Parameters
		----------
		df: dataframe
			Input data.
		links: list of str
			Links.

		Returns
		-------
		out: dataframe
			Input with one or more columns storing the link(s) associated with every row.

		"""

		# links for XML files are added to the `DataFrame`...
		df['XML_link'] = links[::2]

		# ...and so are those for CSV files
		df['CSV_link'] = links[1::2]

		return df

	@staticmethod
	def post_process(df: pd.DataFrame) -> pd.DataFrame:
		"""
		Convenience method to allow applying some custom processing to the `DataFrame` storing the data downloaded.

		Parameters
		----------
		df: dataframe
			Input dataframe to be processed.

		Returns
		-------
		out: dataframe
			Modified dataframe.

		"""

		return df

	def get(self) -> pd.DataFrame:

		# if a previous "feather" file is not found...
		if not self.feather_file.exists():

			# the appointed table at the given URL is parsed into a `DataFrame`
			df = self.parse_html_table()

			# the directory in which zip files are to be downloaded
			downloads_directory = self.downloads_directory / self.name

			# they are afterwards unzipped here
			unzipped_files_directory = downloads_directory / self.unzipped_files_directory

			# CSV links specified in the `DataFrame` are downloaded and uncompressed
			_, uncompressed_files = download.files_list(
				df['CSV_link'], downloads_directory, self.nih_homepage, df['CSV'], unzip_to=unzipped_files_directory)

			# a single `DataFrame` is built from all the CSV files...
			df = util.dataframe_from_csv_files(uncompressed_files, self.parameters['data types'])

			# ...it is post-processed...
			df = self.post_process(df)

			# ...and saved in "feather" format
			df.to_feather(self.feather_file)

		# if a previous "feather" file is found...
		else:

			print(f'loading {self.feather_file}...')

			# ...data are directly loaded from it
			df = pd.read_feather(self.feather_file)

		# the result is cached
		self.df = df

		return df

	def key_columns_to_csv(self, filename: str, drop_nan: bool = True) -> None:
		"""
		Exports `key_columns` to a CSV file.

		Parameters
		----------
		filename: str
			Output file name.
		drop_nan: bool
			Whether or not `nan`s should be excluded prior to saving.

		"""

		to_csv_parameters = dict(path_or_buf=filename, header=True, index=False)

		if drop_nan:

			self.df[self.key_columns].dropna().drop_duplicates().to_csv(**to_csv_parameters)

		else:

			self.df[self.key_columns].drop_duplicates().to_csv(**to_csv_parameters)


class ProjectsDataBunch(DataBunch):

	@staticmethod
	def post_process(df: pd.DataFrame) -> pd.DataFrame:

		df['APPLICATION_TYPE'] = df['APPLICATION_TYPE'].astype('category')

		return df


class PublicationsDataBunch(DataBunch):

	@staticmethod
	def post_process(df: pd.DataFrame) -> pd.DataFrame:

		df['LANG'] = df['LANG'].astype('category')
		df['PUB_YEAR'] = pd.to_datetime(df['PUB_YEAR'], format='%Y')

		return df


class PatentsDataBunch(DataBunch):

	@staticmethod
	def _merge_links(df: pd.DataFrame, links: List[str]) -> pd.DataFrame:

		# only links for XML files are expected...
		df['CSV_link'] = links

		return df
