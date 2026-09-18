"""
Check the Digimon Encyclopedia, parse the page and determine the current
total count of registered Digimon. Compare this against the Count() query
from the API
"""

import os
import sys

import bs4
import requests
from bs4 import BeautifulSoup

REF_URL = "https://digimon.net/reference_en/"


def main():
    base_url = os.getenv("API_BASE_URL")
    if base_url is None:
        print("Failed to get base URL for the API to test against. Failing loudly...")
        sys.exit(1)
    r = requests.get(REF_URL)
    soup = BeautifulSoup(r.text, features="html.parser")

    countStr = ""
    # there's an assumption that these are ordered correctly when iterating.
    for elem in soup.find_all(class_="p-refCountNumList"):
        # get the child elem
        for childElem in elem.children:
            if type(childElem) == bs4.element.Tag:
                alt = childElem.attrs["alt"]
                countStr += str(alt)

    numRegistered = int(countStr)
    print(f"The Digimon Encyclopedia indicates there are {numRegistered} registered Digimon.")

    # query the API and see how many documents are in it
    query = {"query": "query { count }"}
    r = requests.post(f"{base_url}/query", json=query, headers={"Content-Type": "application/json; charset=utf-8"})
    if r.status_code != 200:
        print(f"Expected status code 200, but got {r.status_code}")
        sys.exit(1)
    resp = r.json()
    count_result: int = resp["data"]["count"]
    if count_result != numRegistered:
        print(f"Expected {numRegistered} registered, but database only has {count_result}.")
        sys.exit(1)

    print("Successfully validated count of Digimon registered.")


if __name__ == "__main__":
    main()
