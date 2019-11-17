#! /usr/bin/env python3

import pathlib
import re
import urllib.parse

import parser
import downloader

nih_homepage = 'https://exporter.nih.gov'

current_dir = pathlib.Path.cwd()

projects_url = urllib.parse.urljoin(nih_homepage, 'ExPORTER_Catalog.aspx?sid=1&index=0')
publications_url = urllib.parse.urljoin(nih_homepage, 'ExPORTER_Catalog.aspx?sid=0&index=2')

output_dir = current_dir / 'publications'

re_javascript = re.compile('^javascript.*')

# patterns_to_be_dropped = [re_javascript, re.compile('.*_DUNS_.*\.zip$'), re.compile('.*_PRJFUNDING_.*\.zip$')]
# df = parser.parse_table(projects_url, 12, patterns_to_be_dropped)

patterns_to_be_dropped = [re_javascript, re.compile('.*_AFFLNK_.*\.zip$')]

df = parser.parse_table(publications_url, 12, patterns_to_be_dropped)

# =================================== downloading

output_dir.mkdir(parents=True, exist_ok=True)

for _, row in df.iterrows():

	relative_url = row['CSV_link']

	output_file = output_dir / pathlib.Path(row['CSV_link']).name

	print(f'downloading "{output_file.relative_to(current_dir)}" {row["CSV"]}')

	downloader.file(urllib.parse.urljoin(nih_homepage, row['CSV_link']), output_file)
