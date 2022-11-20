import json
import time
import requests
import pandas as pd
from Collector import DataCollector


def main():
    AUTH = get_api_params()
    # Testing
    # TODO make a simple UI for control/data tracking
    collector = DataCollector(AUTH)
    collector.run_collection(120)


def get_api_params():
    """ Pulls the Endpoint and Authentication key from key.txt. These are laid out in consecutive lines.
    :return: {url: _, key: _}"""
    file = open("key.txt", 'r')
    ENDPOINT = file.readline()[:-1]  # Remove new line char
    API_KEY = file.readline()[:-1]
    return {"url": ENDPOINT, "key": API_KEY}


if __name__ == "__main__":
    # TODO: get args so can run from command line
    main()
