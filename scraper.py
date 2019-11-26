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

nih_homepage = parameters['input']['NIH']['homepage']

projects_relative_path = parameters['input']['NIH']['projects']['relative path']
projects_n_table = parameters['input']['NIH']['projects']['table number']
projects_feather_file = pathlib.Path(parameters['output']['projects']['feather file'])
projects_data_types = parameters['data']['projects']['data types']

publications_relative_path = parameters['input']['NIH']['publications']['relative path']
publications_n_table = parameters['input']['NIH']['publications']['table number']
publications_feather_file = pathlib.Path(parameters['output']['publications']['feather file'])

downloads_directory = pathlib.Path(parameters['output']['downloads directory'])
unzipped_files_directory = pathlib.Path(parameters['output']['unzipped files directory'])

# ====================

projects_url = urllib.parse.urljoin(nih_homepage, projects_relative_path)
publications_url = urllib.parse.urljoin(nih_homepage, publications_relative_path)

re_javascript = re.compile('^javascript.*')

relative_path_list = [publications_relative_path, projects_relative_path]
n_table_list = [publications_n_table, projects_n_table]
file_patterns_to_drop_list = [
	[re_javascript, re.compile('.*_AFFLNK_.*\.zip$')],
	[re_javascript, re.compile('.*_DUNS_.*\.zip$'), re.compile('.*_PRJFUNDING_.*\.zip$')]
]
feather_file_list = [publications_feather_file, projects_feather_file]
dtype_list = [None, projects_data_types]
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


# =================================== downloading

# unzipped_files_directory = downloads_directory / unzipped_files_directory / 'projects'

# if not publications_feather_file.exists():
#
# 	patterns_to_be_dropped = [re_javascript, re.compile('.*_AFFLNK_.*\.zip$')]
# 	df = parser.parse_table(publications_url, publications_n_table, patterns_to_be_dropped)
#
# 	unzipped_files_directory = downloads_directory / unzipped_files_directory / 'publications'
#
# 	# publication files are downloaded *and extracted*
# 	downloaded_files, uncompressed_files = download.files_list(
# 		df['CSV_link'], downloads_directory, nih_homepage, df['CSV'], unzip_to=unzipped_files_directory)
#
# 	df = util.dataframe_from_csv_files(uncompressed_files)
#
# 	df.to_feather(publications_feather_file)
#
# else:
#
# 	df = pd.read_feather(publications_feather_file)

# if not projects_feather_file.exists():
#
# 	patterns_to_be_dropped = [re_javascript, re.compile('.*_DUNS_.*\.zip$'), re.compile('.*_PRJFUNDING_.*\.zip$')]
# 	df = parser.parse_table(projects_url, projects_n_table, patterns_to_be_dropped)
#
# 	unzipped_files_directory = downloads_directory / unzipped_files_directory / 'projects'
#
# 	downloaded_files, uncompressed_files = download.files_list(
# 		df['CSV_link'], downloads_directory, nih_homepage, df['CSV'], unzip_to=unzipped_files_directory)
#
# 	df = util.dataframe_from_csv_files(uncompressed_files, projects_data_types)
#
# 	df.to_feather(projects_feather_file)
#
# else:
#
# 	df = pd.read_feather(projects_feather_file)


breakpoint()

