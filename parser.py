from typing import List, Pattern

import numpy as np
import pandas as pd
import bs4 as bs
import requests


def parse_table(url: str, table_number: int, file_patterns_to_drop: List[Pattern]) -> pd.DataFrame:

	page = requests.get(url)

	# =================================== parsing

	# the web is parsed...
	sp = bs.BeautifulSoup(page.content, 'lxml')

	# ...and *all* the tables extracted
	tables = sp.find_all('table')

	table_of_interest = tables[table_number]

	# the table of interest is further parsed into a Pandas `DataFrame`
	df = pd.read_html(str(table_of_interest), encoding='utf-8', header=0)[0]

	# a numpy array with either links or "no link"s
	links = [np.where(tag.has_attr('href'), tag.get('href'), 'no link') for tag in table_of_interest.find_all('a')]

	# it is turned into a regular Python list
	links = [l.item() for l in links]

	for pattern in file_patterns_to_drop:

		links = [l for l in links if not pattern.match(l)]

	# the links for the XML files are added to the `DataFrame`...
	df['XML_link'] = links[::2]

	# ...and so are those for CSV's
	df['CSV_link'] = links[1::2]

	return df