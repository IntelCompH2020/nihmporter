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
import colors


class DataBunch:

	def __init__(self, parameters: dict) -> None:

		self.parameters = parameters

		# the name of this `DataBunch` is used...
		self.name = parameters['name']

		# ...in naming the output file
		self.binary_output_file = pathlib.Path(parameters['output']['dataframes directory']) / (self.name + '.feather')

		self.nih_homepage = parameters['homepage']
		self.downloads_directory = pathlib.Path(parameters['output']['downloads directory'])
		self.unzipped_files_directory = pathlib.Path(parameters['output']['unzipped files subdirectory'])
		self.key_columns = parameters['key columns']

		# the URL to the appropriate subpage within the NIH homepage
		self.url = urllib.parse.urljoin(parameters['homepage'], parameters['relative path'])

		# regular expressions defining the files that must be ignored
		self.re_files_to_be_ignored = [re.compile(expr) for expr in parameters['filename patterns to be ignored']]

		# to act as cache
		self.df: Optional[pd.DataFrame] = None

	def load_df(self) -> pd.DataFrame:

		return pd.read_feather(self.binary_output_file)

	def save_df(self, df: pd.DataFrame) -> None:

		df.to_feather(self.binary_output_file)

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

		# it is known beforehand the "position" of the table of interest...
		table_of_interest = tables[self.parameters['table number']]

		# ...still, when asked to read html, `Pandas` returns a list of `DataFrame`s
		table_of_interest_dfs = pd.read_html(str(table_of_interest), encoding='utf-8', header=0)

		# if there is more than one Pandas table in the selected HTML table, then something is off
		assert len(table_of_interest_dfs) == 1,\
			f'the table for "{self.name}" has most likely moved to a different position in the webpage'

		# the only element in the list above is selected
		df = table_of_interest_dfs[0]

		# a numpy array with either links or "no link"s
		links = [np.where(tag.has_attr('href'), tag.get('href'), 'no link') for tag in table_of_interest.find_all('a')]

		# it is turned into a regular Python list and stored as an instance attribute for later use
		links = [l.item() for l in links]

		# every regular expression that is NOT considered an URL...
		for pattern in self.re_files_to_be_ignored:

			# ...is used to trim the list
			links = [l for l in links if not pattern.match(l)]

		# pd.set_option('display.max_colwidth', 1700)
		# foo = self._merge_links(df, links)
		# foo[['Project File Name', 'CSV_link']]

		return self._merge_links(df, links)

	@classmethod
	def _merge_links(cls, df: pd.DataFrame, links: List[str]) -> pd.DataFrame:
		"""
		Incorporate the passed `links` to the given `DataFrame`. The latter is modified in-place.

		Parameters
		----------
		df: `DataFrame`
			Input data.
		links: list of str
			Links.

		Returns
		-------
		out: `DataFrame`
			Input `DataFrame` with two more columns storing the links associated with every row.

		"""

		assert len(links) % 2 == 0, 'the number of links is not even (XML & CSV)'

		# links for XML files are added to the `DataFrame`...
		df['XML_link'] = links[::2]

		# ...and so are those for CSV files
		df['CSV_link'] = links[1::2]

		return df

	@staticmethod
	def _matching_year_merge_links(
			df: pd.DataFrame, links: List[str], year_in_project_file_name_pattern: str,
			type_format_year_in_zip_file_name_pattern: str, type_of_interest: str) -> pd.DataFrame:
		"""
		Incorporate the passed `links` to the given `DataFrame` while accounting for the year (as stated in the
		"Project File Name" field, and the name of the file to be downloaded). The input `DataFrame` is modified in-place.

		Parameters
		----------
		df : `DataFrame`
			Input data.
		links : list of str
			Links.
		year_in_project_file_name_pattern : str
			Regular expression for capturing the year in the field "Project File Name".
		type_format_year_in_zip_file_name_pattern : str
			Regular expression for capturing the "type", "format" and year of the data in the name of the file to be
			downloaded.
		type_of_interest : str
			The type of interest as captured by `year_in_project_file_name_pattern`
			(notice the latter might capture more than one type using a "| construction")

		Returns
		-------
		out: `DataFrame`
			Input `DataFrame` with two more columns storing the links associated with every row.

		"""

		year_series = df['Project File Name'].str.extract(year_in_project_file_name_pattern)[0].astype(pd.StringDtype())

		# there are no duplicates
		assert not year_series.duplicated().any()

		# it is set as index
		df.set_index(year_series, inplace=True)

		# for exploiting Pandas capabilities, the list of links is turned into a `Series` of strings
		raw_links_series = pd.Series(links, dtype=pd.StringDtype()).rename('URL')

		# links are parsed
		# type: PUBlication or AFFiLiation
		# format: Csv or Xml
		links_type_format_year_df = raw_links_series.str.extract(type_format_year_in_zip_file_name_pattern).rename(
			{0: 'type', 1: 'format', 2: 'year'}, axis=1)

		# boolean indicating which rows contain at list one link
		contains_links = ~links_type_format_year_df.isna().all(axis=1)

		# links are concatenated with the extracted fields, rows without links are dropped, and year is used as index
		links_df = pd.concat([raw_links_series, links_type_format_year_df], axis=1)[contains_links].set_index('year')

		is_csv_about_pub = (links_df['type'] == type_of_interest) & (links_df['format'] == 'C')
		df['CSV_link'] = np.nan
		df.loc[:, 'CSV_link'] = links_df[is_csv_about_pub]['URL']

		is_xml_about_pub = (links_df['type'] == type_of_interest) & (links_df['format'] == 'X')
		df['XML_link'] = np.nan
		df.loc[:, 'XML_link'] = links_df[is_xml_about_pub]['URL']

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
		"""
		Gets the data associated to this `DataBunch`.

		Returns
		-------
		out: dataframe
			Requested data.

		"""

		# if a previous "binary output" file is not found...
		if not self.binary_output_file.exists():

			# the appointed table at the given URL is parsed into a `DataFrame`
			html_table_df = self.parse_html_table()

			# the directory in which zip files are to be downloaded
			downloads_directory = self.downloads_directory / self.name

			# they are afterwards unzipped here
			unzipped_files_directory = downloads_directory / self.unzipped_files_directory

			# CSV links specified in the `DataFrame` are downloaded and uncompressed
			_, uncompressed_files = download.files_list(
				html_table_df['CSV_link'], downloads_directory, self.nih_homepage, html_table_df['CSV'],
				unzip_to=unzipped_files_directory)

			# a single `DataFrame` is built from all the CSV files...
			csv_df = util.dataframe_from_csv_files(uncompressed_files, self.parameters['data types'])

			# ...it is post-processed...
			csv_df = self.post_process(csv_df)

			print(f'{colors.info}writing {colors.reset}"{self.binary_output_file}"')

			# ...and saved
			self.save_df(csv_df)

		# if a previous "binary output" file is found...
		else:

			print(f'{colors.info}loading {colors.reset}"{self.binary_output_file}"')

			# ...data are directly loaded from it
			csv_df = self.load_df()

		# the result is cached
		self.df = csv_df

		return csv_df

	def key_columns_to_csv(self, filename: str = None, drop_nan: bool = True) -> None:
		"""
		Exports `key_columns` to a CSV file.

		Parameters
		----------
		filename: str, optional
			Output file name.
		drop_nan: bool
			Whether or not `nan`s should be excluded prior to saving.

		"""

		# if there are no key columns...
		if not self.key_columns:

			print(f'{colors.warning}CSV file for {colors.reset}"{self.name}"{colors.warning} would be empty...skipping')

			return

		# if a file name was not provided...
		if not filename:

			# ...the default one is used
			filename = self.binary_output_file.with_suffix('.csv')

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

	year_in_project_file_name_pattern = r'.*year (\d{4})'
	re_year_in_author_affiliations = re.compile(r'.*_AFFLNK_C_(\d{4})\.zip$')

	@staticmethod
	def post_process(df: pd.DataFrame) -> pd.DataFrame:

		df['LANG'] = df['LANG'].astype('category')
		df['PUB_YEAR'] = pd.to_datetime(df['PUB_YEAR'], format='%Y')

		return df

	@classmethod
	def _merge_links(cls, df: pd.DataFrame, links: List[str]) -> pd.DataFrame:

		# # for storing relevant links and their corresponding year below
		# affiliations_links, affiliations_years = [], []
		#
		# # every link...
		# for l in links:
		#
		# 	# ...that matches the pattern for an "affiliations file"...
		# 	if m := cls.re_year_in_author_affiliations.match(l):
		#
		# 		# ...is stored...
		# 		affiliations_links.append(l)
		#
		# 		# ...along with its corresponding year
		# 		affiliations_years.append(m.group(1))

		# links for CSV's and XML's are added to the `DataFrame`
		cls._matching_year_merge_links(
			df, links, cls.year_in_project_file_name_pattern, r'_(PUB|AFFLNK)_([CX])_(\d{4}).zip', r'PUB')

		# # a new column for "affiliations" is added to the `DataFrame`
		# df['Author Affiliations'] = np.nan
		#
		# # it is only filled in at the appropriate rows
		# df.loc[affiliations_years, 'Author Affiliations'] = affiliations_links

		return df


class PublicationsAuthorAffiliationsDataBunch(DataBunch):

	@staticmethod
	def post_process(df: pd.DataFrame) -> pd.DataFrame:

		df['AUTH_TRANS_ID'] = df['AUTH_TRANS_ID'].astype(pd.StringDtype())

		return df

	@classmethod
	def _merge_links(cls, df: pd.DataFrame, links: List[str]) -> pd.DataFrame:

		cls._matching_year_merge_links(
			df, links, PublicationsDataBunch.year_in_project_file_name_pattern,
			r'.*_(AFFLNK)_(C)_(\d{4})\.zip$', r'AFFLNK')

		# rows that have nothing to download must be dropped
		df.drop(df[df['CSV_link'].isna()].index, inplace=True)

		# this size doesn't correspond to the authors' affiliations file (but to the associated publications one)
		df['CSV'] = ''

		return df


class PatentsDataBunch(DataBunch):

	@classmethod
	def _merge_links(cls, df: pd.DataFrame, links: List[str]) -> pd.DataFrame:

		# only links for XML files are expected...
		df['CSV_link'] = links

		return df


class AbstractsDataBunch(DataBunch):

	def __init__(self, parameters: dict) -> None:

		super().__init__(parameters)

		self.binary_output_file = self.binary_output_file.with_suffix('.pickle')

	def load_df(self) -> pd.DataFrame:

		return pd.read_pickle(self.binary_output_file)

	def save_df(self, df: pd.DataFrame) -> None:

		df.to_pickle(self.binary_output_file)


class LinksDataBunch(DataBunch):

	year_in_project_file_name_pattern = r'.*Publications (\d{4}) link tables'

	@classmethod
	def _merge_links(cls, df: pd.DataFrame, links: List[str]) -> pd.DataFrame:

		return cls._matching_year_merge_links(
			df, links, cls.year_in_project_file_name_pattern, r'_(PUBLNK)_([CX])_(\d{4}).zip', r'PUBLNK')
