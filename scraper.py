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

# this dictionary will store all the composed `DataFrame`s
res = dict()

# for both projects- and publications-related parameters...
for p in [parameters['projects'], parameters['publications'], parameters['link tables'], parameters['patents']]:

	# if a previous "feather" file is not found...
	if not pathlib.Path(p['feather file']).exists():

		# the URL to the appropriate subpage within the NIH homepage
		url = urllib.parse.urljoin(nih_homepage, p['relative path'])

		# regular expressions defining the files that must be ignored
		regular_expressions = [re.compile(expr) for expr in p['filename patterns to be ignored']]

		parse_table_extra_parameters = p['extra parameters'].get('parse_table', {}) if 'extra parameters' in p else {}

		# the appointed table at the above URL is parsed into a `DataFrame`
		df = parser.parse_table(url, p['table number'], regular_expressions, **parse_table_extra_parameters)

		# the directory in which data files are to be unzipped
		unzipped_files_directory = downloads_directory / unzipped_files_directory / p['name']

		# CSV links specified in the `DataFrame` are downloaded and uncompressed
		downloaded_files, uncompressed_files = download.files_list(
			df['CSV_link'], downloads_directory, nih_homepage, df['CSV'], unzip_to=unzipped_files_directory)

		# a single `DataFrame` is built from all the CSV files...
		df = util.dataframe_from_csv_files(uncompressed_files, p['data types'])

		# ...and saved in "feather" format
		df.to_feather(p['feather file'])

	# if a previous "feather" file is found...
	else:

		print(f'loading {p["feather file"]}...')

		# ...data are directly loaded from it
		df = pd.read_feather(p['feather file'])

	# the loaded/built `DataFrame` is added to the dictionary at the key specified by the "name" parameter
	res[p['name']] = df

# for the sake of convenience
projects = res['projects']
publications = res['publications']
link = res['link']
patents = res['patents']

# some curating
publications['LANG'] = publications['LANG'].astype('category')
publications['PUB_YEAR'] = pd.to_datetime(publications['PUB_YEAR'], format='%Y')

# grouped_projects = projects.groupby('CORE_PROJECT_NUM')
# grouped_projects.get_group('ZIJAR041172')

breakpoint()

# l = link.iloc[0]
# projects['CORE_PROJECT_NUM'] == l['PROJECT_NUMBER']
# publications['PMID'] == l['PMID']
