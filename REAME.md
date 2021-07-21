*nihmporter* is Python software to download and *pack* the data published by the [National Institute of Health](https://exporter.nih.gov/).

# Installation

You can use [make\_conda\_environment.sh](https://github.com/manuvazquez/nihmporter/blob/master/make_conda_environment.sh) to build a proper [Anaconda](https://anaconda.org/) environment (by default, named `nih`), or inspect it to see the exact requirements.

# Usage

Activate the above environment and run

```
# after activating the appropriate conda environement
./import.py
```

It should result in some [*feather*](https://arrow.apache.org/docs/python/feather.html)/*pickle* (as of July 2021, huge feather files cause memory issues) files, each one storing a [Pandas](https://pandas.pydata.org/) dataframe.

The script also produces a bunch of *csv* files which subset the above *feather*/*pickle* files into some data exploited by the (extra) utiliy `connectivity_stats.py`.
