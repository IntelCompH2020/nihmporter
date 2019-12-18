#! /usr/bin/env python3

import pathlib

import yaml
import pandas as pd

# ==================== parameters reading

# parameters are read
with open('parameters.yaml') as yaml_data:

	# the parameters file is read to memory
	parameters = yaml.load(yaml_data, Loader=yaml.FullLoader)

# ---

projects_csv_file = pathlib.Path(parameters['projects']['name'] + '.csv')
publications_csv_file = pathlib.Path(parameters['publications']['name'] + '.csv')
link_csv_file = pathlib.Path(parameters['link tables']['name'] + '.csv')
patents_csv_file = pathlib.Path(parameters['patents']['name'] + '.csv')

for file in [projects_csv_file, publications_csv_file, link_csv_file]:

	if not file.exists():

		print(f"file {file} doesn't exist, please run the main program first")

		raise SystemExit

projects = pd.read_csv(projects_csv_file)
publications = pd.read_csv(publications_csv_file)
link = pd.read_csv(link_csv_file)
patents = pd.read_csv(patents_csv_file)

n_projects = len(projects)
n_publications = len(publications)
n_patents = len(patents)

# --- projects and publications

projects_link = projects.merge(link, how='outer', left_on='CORE_PROJECT_NUM', right_on='PROJECT_NUMBER', indicator=True)

pub_linked_to_inexistent_proj = (projects_link._merge == "right_only").sum()

print(
	f'# publications linked to inexistent projects: {pub_linked_to_inexistent_proj} '
	f'(out of {n_publications} *linked* publications)'
)

proj_with_no_linked_pub = (projects_link._merge == "left_only").sum()

print(f'# projects with no linked publications: {proj_with_no_linked_pub} (out of {n_projects})')

# --- publications

publications_link = publications.merge(link, how='outer', on='PMID', indicator=True)

n_orphan_publications = (publications_link._merge != "both").sum()

print(f'# publications with not linked *data*: {n_orphan_publications}')

# --- projects and patents

patents_projects = patents.merge(projects, how='outer', left_on='PROJECT_ID', right_on='CORE_PROJECT_NUM', indicator=True)

pat_with_no_linked_proj = (patents_projects._merge == "left_only").sum()

print(f'# patents with no linked project: {pat_with_no_linked_proj} (out of {n_patents})')

proj_with_no_linked_pat = (patents_projects._merge == "right_only").sum()

print(f'# projects with no linked patents: {proj_with_no_linked_pat} (out of {n_projects})')

# ---

# publications_title = pd.read_feather(publications_csv_file.with_suffix('.feather'), columns=['PMID', 'PUB_TITLE'])
# merged = projects_link.merge(publications_title, how='outer', on='PMID')