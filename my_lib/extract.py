"""
Extract data from a url and save as a file
"""

import requests


def extract(
    url: str,
    file_name: str,
):
    """ "Extract a url to a file path"""
    file_path = "data/" + file_name
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return "Extract Successful"
