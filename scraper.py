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

publications_relative_path = parameters['input']['NIH']['publications']['relative path']
publications_n_table = parameters['input']['NIH']['publications']['table number']
publications_feather_file = pathlib.Path(parameters['output']['publications']['feather file'])

publications_relative_path = parameters['input']['NIH']['publications']['relative path']

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

# unzipped_files_directory = downloads_directory / unzipped_files_directory / 'projects'

if not publications_feather_file.exists():

	unzipped_files_directory = downloads_directory / unzipped_files_directory / 'publications'

	# publication files are downloaded *and extracted*
	downloaded_files, uncompressed_files = download.files_list(
		df['CSV_link'], downloads_directory, nih_homepage, df['CSV'], unzip_to=unzipped_files_directory)

	df = util.dataframe_from_csv_files(uncompressed_files)

	df.to_feather(publications_feather_file)

else:

	df = pd.read_feather(publications_feather_file)

breakpoint()

