import pathlib
import requests
import zipfile
import urllib.parse
from typing import Union, Optional, Collection, List, Tuple

current_dir = pathlib.Path.cwd()


def file(url: str, output_file: Union[str, pathlib.Path]) -> None:
	"""
	Download a file from an URL

	Parameters
	----------
	url: str
		The URL.
	output_file: str or pathlib object
		The path to the output file that will be created.

	"""

	read_url = requests.get(url)

	with open(output_file, 'wb') as f:

		f.write(read_url.content)


def files_list(
		files_relative_paths: Collection[str], downloads_directory: Union[str, pathlib.Path], homepage: str,
		sizes: Optional[Collection[str]] = None,
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
	sizes: iterable over str, optional
		Sizes of the files.
	unzip_to: str, optional
		The directory to which the downloaded files should be unzipped.

	Returns
	-------
	downloaded_files: list of pathlib.Path
		The paths of the downloaded files.
	unzipped_files: list of pathlib.Path
		The paths of the uncompressed files if `unzip_to` was passed.

	"""

	# if a `Collection` of sizes was passed...
	if sizes is not None:

		# ...we make sure there is one size per file
		assert len(files_relative_paths) ==  len(sizes)

	# if no `Collection` of sizes was passed...
	else:

		# ...a generator of empty strings is used instead
		sizes = ("" for _ in range(len(files_relative_paths)))

	output_dir = current_dir / downloads_directory

	output_dir.mkdir(parents=True, exist_ok=True)

	# a list with the files downloaded
	downloaded_files = []

	# if a directory to unzip the files to was passed...
	if unzip_to is not None:

		# ...the list of uncompressed files is initialized
		unzipped_files = []

	for f, size in zip(files_relative_paths, sizes):

		output_file = output_dir / pathlib.Path(f).name

		# if the file has not been previously downloaded...
		if not output_file.exists():

			print(f'downloading "{output_file.relative_to(current_dir)}" {size}')

			# ...it is now
			file(urllib.parse.urljoin(homepage, f), output_file)

		else:

			print(f'found "{output_file.relative_to(current_dir)}" {size}')

		# if a directory to unzip the files to was passed...
		if unzip_to is not None:

			with zipfile.ZipFile(output_file) as downloaded_zip:

				downloaded_zip.extractall(path=unzip_to)

				unzipped_files.extend([unzip_to / f for f in downloaded_zip.namelist()])

		# the output file path is added to the list of downloaded files
		downloaded_files.append(output_file)

	if unzip_to:

		return downloaded_files, unzipped_files

	else:

		return downloaded_files