#! /usr/bin/env python3

import pathlib
import re
import urllib.parse

import yaml
import pandas as pd

import parser
import download
import util

# ==================== parameters reading

# parameters are read
with open('parameters.yaml') as yaml_data:

	# the parameters file is read to memory
	parameters = yaml.load(yaml_data, Loader=yaml.FullLoader)

# ---

# common parameters
nih_homepage = parameters['input']['NIH']['homepage']
downloads_directory = pathlib.Path(parameters['output']['downloads directory'])
unzipped_files_directory = pathlib.Path(parameters['output']['unzipped files directory'])

# projects
projects_relative_path = parameters['input']['NIH']['projects']['relative path']
projects_n_table = parameters['input']['NIH']['projects']['table number']
projects_feather_file = pathlib.Path(parameters['output']['projects']['feather file'])
projects_data_types = parameters['data']['projects']['data types']

# publications
publications_relative_path = parameters['input']['NIH']['publications']['relative path']
publications_n_table = parameters['input']['NIH']['publications']['table number']
publications_feather_file = pathlib.Path(parameters['output']['publications']['feather file'])
publications_data_types = parameters['data']['publications']['data types']

# ====================

re_javascript = re.compile('^javascript.*')

relative_path_list = [publications_relative_path, projects_relative_path]
n_table_list = [publications_n_table, projects_n_table]
file_patterns_to_drop_list = [
	[re_javascript, re.compile('.*_AFFLNK_.*\.zip$')],
	[re_javascript, re.compile('.*_DUNS_.*\.zip$'), re.compile('.*_PRJFUNDING_.*\.zip$')]
]
feather_file_list = [publications_feather_file, projects_feather_file]
dtype_list = [publications_data_types, projects_data_types]
names_list = ['publications', 'projects']

res = dict()

for relative_path, n_table, file_patterns_to_drop, feather_file, dtype, name in zip(
		relative_path_list, n_table_list, file_patterns_to_drop_list, feather_file_list, dtype_list, names_list):

	if not feather_file.exists():

		url = urllib.parse.urljoin(nih_homepage, relative_path)

		df = parser.parse_table(url, n_table, file_patterns_to_drop)

		unzipped_files_directory = downloads_directory / unzipped_files_directory / name

		downloaded_files, uncompressed_files = download.files_list(
			df['CSV_link'], downloads_directory, nih_homepage, df['CSV'], unzip_to=unzipped_files_directory)

		df = util.dataframe_from_csv_files(uncompressed_files, dtype)

		df.to_feather(feather_file)

	else:

		df = pd.read_feather(feather_file)

	res[name] = df

breakpoint()

