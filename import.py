#! /usr/bin/env python3

import yaml

import nih

# ==================== parameters reading

# parameters are read
with open('parameters.yaml') as yaml_data:

	# the parameters file is read to memory
	parameters = yaml.load(yaml_data, Loader=yaml.FullLoader)

# ---

projects = nih.ProjectsDataBunch({**parameters['common'], **parameters['projects']})
publications = nih.PublicationsDataBunch({**parameters['common'], **parameters['publications']})
links = nih.DataBunch({**parameters['common'], **parameters['link tables']})
patents = nih.PatentsDataBunch({**parameters['common'], **parameters['patents']})
abstracts = nih.AbstractsDataBunch({**parameters['common'], **parameters['abstracts']})

# for each one of the above "data bunch"s...
for data_bunch in [projects, publications, links, patents, abstracts]:

	# ...data is actually downloaded (if necessary)
	data_bunch.get()

	# "key columns" are exported to a CSV file
	data_bunch.key_columns_to_csv()

	# in order to avoid "out of memory" errors
	del data_bunch

# grouped_projects = projects.df.groupby('CORE_PROJECT_NUM')
# grouped_projects.get_group('ZIJAR041172')
