#! /usr/bin/env python3

import pathlib
import re
import urllib.parse
import zipfile

import yaml

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
projects_links_column = parameters['input']['NIH']['projects']['links column']

publications_relative_path = parameters['input']['NIH']['publications']['relative path']
publications_n_table = parameters['input']['NIH']['publications']['table number']
publications_links_column = parameters['input']['NIH']['publications']['links column']

downloads_directory = pathlib.Path(parameters['output']['downloads directory'])

# ====================

current_dir = pathlib.Path.cwd()

projects_url = urllib.parse.urljoin(nih_homepage, projects_relative_path)
publications_url = urllib.parse.urljoin(nih_homepage, publications_relative_path)

re_javascript = re.compile('^javascript.*')

# patterns_to_be_dropped = [re_javascript, re.compile('.*_DUNS_.*\.zip$'), re.compile('.*_PRJFUNDING_.*\.zip$')]
# df = parser.parse_table(projects_url, projects_n_table, patterns_to_be_dropped)

patterns_to_be_dropped = [re_javascript, re.compile('.*_AFFLNK_.*\.zip$')]

df = parser.parse_table(publications_url, publications_n_table, patterns_to_be_dropped)

# =================================== downloading

downloaded_files = download.files_list(df[publications_links_column], downloads_directory, nih_homepage, df['CSV'])

# with zipfile.ZipFile(downloaded_files[0]) as downloaded_zip:
#
# 	downloaded_zip.extractall(path=downloads_directory / 'unzipped')
