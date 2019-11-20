#! /usr/bin/env python3

import pathlib
import re
import urllib.parse

import yaml
import pandas as pd

import parser
import download

# ==================== parameters reading

# parameters are read
with open('parameters.yaml') as yaml_data:

	# the parameters file is read to memory
	parameters = yaml.load(yaml_data, Loader=yaml.FullLoader)

# ---

nih_homepage = parameters['input']['NIH']['homepage']

projects_relative_path = parameters['input']['NIH']['projects']['relative path']
projects_n_table = parameters['input']['NIH']['projects']['table number']

publications_relative_path = parameters['input']['NIH']['publications']['relative path']
publications_n_table = parameters['input']['NIH']['publications']['table number']
publications_feather_file = parameters['output']['publications']['feather file']

downloads_directory = pathlib.Path(parameters['output']['downloads directory'])
unzipped_files_directory = pathlib.Path(parameters['output']['unzipped files directory'])

# ====================

projects_url = urllib.parse.urljoin(nih_homepage, projects_relative_path)
publications_url = urllib.parse.urljoin(nih_homepage, publications_relative_path)

re_javascript = re.compile('^javascript.*')

# patterns_to_be_dropped = [re_javascript, re.compile('.*_DUNS_.*\.zip$'), re.compile('.*_PRJFUNDING_.*\.zip$')]
# df = parser.parse_table(projects_url, projects_n_table, patterns_to_be_dropped)

patterns_to_be_dropped = [re_javascript, re.compile('.*_AFFLNK_.*\.zip$')]

df = parser.parse_table(publications_url, publications_n_table, patterns_to_be_dropped)

# =================================== downloading

# publication files are downloaded *and extracted*
downloaded_files = download.files_list(
	df['CSV_link'], downloads_directory, nih_homepage, df['CSV'], unzip_to=unzipped_files_directory)
# downloaded_files = download.files_list(df['CSV_link'], downloads_directory, nih_homepage, df['CSV'])

unzipped_files_directory = downloads_directory / unzipped_files_directory

# a list with a `DataFrame` for every csv file
per_year_dfs = []

# for every csv file in the unzipped files directory...
for csv_file in unzipped_files_directory.glob('*.csv'):

	# ...the file is read into a `DataFrame` and added to the above list
	per_year_dfs.append(pd.read_csv(csv_file, encoding='iso8859_15'))

# all the `DataFrame`s are concatenated together
df = pd.concat(per_year_dfs, axis=0)

breakpoint()

# in order to save the `DataFrame` in a feather file, we need to reset the index
df.reset_index(inplace=True)

# the old index was artificial and not actually needed
df.drop(['index'], axis=1, inplace=True)

df.to_feather(publications_feather_file)

