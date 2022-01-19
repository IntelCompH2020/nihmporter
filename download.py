import pathlib

import pandas as pd
import requests
import zipfile
import urllib.parse
from typing import Union, Optional, Collection, List, Tuple

import util

current_dir = pathlib.Path.cwd()


def file(url: str, output_file: Union[str, pathlib.Path]) -> None:
	"""
	Download a file from a URL.

	Parameters
	----------
	url: str
		The URL.
	output_file: str or pathlib object
		The path to the output file that will be created.

	"""

	# URL is requested
	read_url = requests.get(url)

	# the output file is open for writing...
	with open(output_file, 'wb') as f:

		# ...and the content of the URL written
		f.write(read_url.content)


def files_list(
		files_relative_paths: Collection[str], downloads_directory: Union[str, pathlib.Path], homepage: str,
		last_update_dates: Collection[pd.Timestamp], sizes: Optional[Collection[str]] = None,
		unzip_to: Optional[str] = None) -> Union[Tuple[List[pathlib.Path], List[pathlib.Path]], List[pathlib.Path]]:
	"""
	Downloads the URLs.

	Parameters
	----------
	files_relative_paths: iterable over str
		Sequence of *relative* paths to files.
	downloads_directory: str or pathlib object
		Directory where the files will be downloaded.
	homepage: str
		Base URL with respect to which the `relative_paths` are specified.
	last_update_dates: iterable over `pd.Timestamps`
		Date a file was last updated in the server.
	sizes: iterable over str, optional
		Sizes of the files.
	unzip_to: str, optional
		The directory to which the downloaded files should be unzipped.

	Returns
	-------
	downloaded_files: list of pathlib.Path
		The paths of the downloaded files.
	unzipped_files: list of pathlib.Path
		The paths of the uncompressed files *if* `unzip_to` was passed.

	"""

	# as many dates as files
	assert len(files_relative_paths) == len(last_update_dates)

	# if a `Collection` of sizes was passed...
	if sizes is not None:

		# ...we make sure there is one size per file
		assert len(files_relative_paths) == len(sizes)

	# if no `Collection` of sizes was passed...
	else:

		# ...a generator of empty strings is used instead
		sizes = ("" for _ in range(len(files_relative_paths)))

	# the output directory is obtained from the current one and the passed `downloads_directory`
	output_dir = current_dir / downloads_directory

	# it is created
	output_dir.mkdir(parents=True, exist_ok=True)

	# a list for the downloaded files
	downloaded_files = []

	# if a directory to unzip the files to was passed...
	if unzip_to is not None:

		# ...a list for uncompressed files is initialized
		unzipped_files = []

	# for every file, along with its size and last update date...
	for f, size, last_update in zip(files_relative_paths, sizes, last_update_dates):

		output_file = output_dir / pathlib.Path(f).name

		file_was_downloaded = False

		# if the file has not been previously downloaded...
		if not output_file.exists():

			print(f'downloading "{output_file.relative_to(current_dir)}" {size}')

			# ...it is now
			file(urllib.parse.urljoin(homepage, f), output_file)

			file_was_downloaded = True

		else:

			modification_date = util.modification_date_from_path(output_file)

			# if the file in the server was updated *after* the (modification) date recorded in the downloaded file...
			if last_update > modification_date:

				print(f'updating "{output_file.relative_to(current_dir)}" {size}')

				# ...it must be re-downloaded
				file(urllib.parse.urljoin(homepage, f), output_file)

			# if the file in the server was updated *before* the (modification) date recorded in the downloaded file...
			else:

				print(f'found up-to-date "{output_file.relative_to(current_dir)}" {size}')

		# if a directory to unzip the files to was passed...
		if unzip_to is not None:

			with zipfile.ZipFile(output_file) as downloaded_zip:

				# only if the zip file was downloaded...
				if file_was_downloaded:

					# ...it is decompressed
					downloaded_zip.extractall(path=unzip_to)

				unzipped_files.extend([unzip_to / f for f in downloaded_zip.namelist()])

		# the output file path is added to the list of downloaded files
		downloaded_files.append(output_file)

	# if a directory to unzip the files to was passed...
	if unzip_to is not None:

		# ...uncompressed files are also returned
		return downloaded_files, unzipped_files

	# if a directory to unzip the files to was NOT passed...
	else:

		# ...only downloaded files are returned
		return downloaded_files
