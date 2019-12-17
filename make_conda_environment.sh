#!/bin/bash

# only required if "anaconda" is not in the path
source $HOME/anaconda3/etc/profile.d/conda.sh

NAME=nih

conda create --yes -n $NAME python=3 pyyaml colorama pandas pyarrow ipdb jupyterlab lxml beautifulsoup4 requests -c defaults -c conda-forge

echo new environment is \"$NAME\"
