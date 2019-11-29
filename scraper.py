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
nih_homepage = parameters['homepage']
downloads_directory = pathlib.Path(parameters['output']['downloads directory'])
unzipped_files_directory = pathlib.Path(parameters['output']['unzipped files directory'])

res = dict()

for p in [parameters['projects'], parameters['publications']]:

	if not pathlib.Path(p['feather file']).exists():

		url = urllib.parse.urljoin(nih_homepage, p['relative path'])

		regular_expressions = [re.compile(expr) for expr in p['filename patterns to be ignored']]

		df = parser.parse_table(url, p['table number'], regular_expressions)

		unzipped_files_directory = downloads_directory / unzipped_files_directory / p['name']

		downloaded_files, uncompressed_files = download.files_list(
			df['CSV_link'], downloads_directory, nih_homepage, df['CSV'], unzip_to=unzipped_files_directory)

		df = util.dataframe_from_csv_files(uncompressed_files, p['data types'])

		df.to_feather(p['feather file'])

	else:

		print(f'loading {p["feather file"]}...')

		df = pd.read_feather(p['feather file'])

	res[p['name']] = df

breakpoint()

