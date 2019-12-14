#! /usr/bin/env python3

import yaml

import nih

# ==================== parameters reading

# parameters are read
with open('parameters.yaml') as yaml_data:

	# the parameters file is read to memory
	parameters = yaml.load(yaml_data, Loader=yaml.FullLoader)

# ---

projects_df = nih.DataBunch({**parameters['common'], **parameters['projects']})()
publications_df = nih.PublicationsDataBunch({**parameters['common'], **parameters['publications']})()
links_df = nih.DataBunch({**parameters['common'], **parameters['link tables']})()
patents_df = nih.PatentsDataBunch({**parameters['common'], **parameters['patents']})()

# grouped_projects = projects_df.groupby('CORE_PROJECT_NUM')
# grouped_projects.get_group('ZIJAR041172')

breakpoint()

# l = link.iloc[0]
# projects_df['CORE_PROJECT_NUM'] == l['PROJECT_NUMBER']
# publications_df['PMID'] == l['PMID']
