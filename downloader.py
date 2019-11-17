import requests


def file(url: str, output_file: str) -> None:

	read_url = requests.get(url)

	with open(output_file, 'wb') as f:

		f.write(read_url.content)
