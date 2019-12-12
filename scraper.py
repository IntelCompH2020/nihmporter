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

# for both projects- and publications-related parameters...
for p in [parameters['projects'], parameters['publications'], parameters['link tables']]:

	# if a previous "feather" file is not found...
	if not pathlib.Path(p['feather file']).exists():

		# the URL to the appropriate subpage within the NIH homepage
		url = urllib.parse.urljoin(nih_homepage, p['relative path'])

		# regular expressions defining the files that must be ignored
		regular_expressions = [re.compile(expr) for expr in p['filename patterns to be ignored']]

		# the appointed table at the above URL is parsed into a `DataFrame`
		df = parser.parse_table(url, p['table number'], regular_expressions)

		# the directory in which data files are to be unzipped
		unzipped_files_directory = downloads_directory / unzipped_files_directory / p['name']

		# CSV links specified in the `DataFrame` are downloaded and uncompressed
		downloaded_files, uncompressed_files = download.files_list(
			df['CSV_link'], downloads_directory, nih_homepage, df['CSV'], unzip_to=unzipped_files_directory)

		df = util.dataframe_from_csv_files(uncompressed_files, p['data types'])

		df.to_feather(p['feather file'])

	# if a previous "feather" file is found...
	else:

		print(f'loading {p["feather file"]}...')

		df = pd.read_feather(p['feather file'])

	res[p['name']] = df

# for the sake of convenience
projects = res['projects']
publications = res['publications']
link = res['link']

# some curating
publications['LANG'] = publications['LANG'].astype('category')
publications['PUB_YEAR'] = pd.to_datetime(publications['PUB_YEAR'], format='%Y')

breakpoint()

# l = link.iloc[0]
# projects['CORE_PROJECT_NUM'] == l['PROJECT_NUMBER']
# publications['PMID'] == l['PMID']

