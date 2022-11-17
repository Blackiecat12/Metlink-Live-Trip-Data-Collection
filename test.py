import json
import time
import requests
import pandas as pd
from trip import trip_record


def main():
    AUTH = get_api_params()
    records = {}
    for i in range(5):
        entities = get_tripupdates(AUTH)
        for entity in entities:
            e_id = entity['trip_update']['trip']['trip_id']
            if records.get(e_id) == None:
                records[e_id] = trip_record(entity)
            else:
                records[e_id].update(entity)
        if i < 4:
            time.sleep(90)
        print(f"Done {i+1}/5")
    for record in records.values():
        record.debug()


def get_tripupdates(AUTH):
    """ Currently a test function to get live trip updates from metlink api. """
    link = "/gtfs-rt/tripupdates"
    response = metlink_get(link, AUTH)
    if response is None:
        return

    # Update the dictionary
    entities = response.json()["entity"]
    return entities


def metlink_get(url, AUTH):
    """ Sends the request to the given URL. All parameters are part of the url so the rest is constant.
    :param url: url to be added to the base
    :param AUTH: Authorisation params """
    header = {
        "accept": "application/json",
        "x-api-key": AUTH['key']
    }
    r = requests.get(AUTH['url']+url, headers=header)

    # Check it succeeded
    if r.status_code != 200:
        print(f"Requesting {url} failed with code {r.status_code}")
        return None
    return r


def jprint(obj):
    """
    Prints the formatted json object
    :param obj: Json object
    """
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


def get_api_params():
    """ Pulls the Endpoint and Authentication key from key.txt. These are laid out in consecutive lines.
    :return: {url: _, key: _}"""
    file = open("key.txt", 'r')
    ENDPOINT = file.readline()[:-1]  # Remove new line char
    API_KEY = file.readline()[:-1]
    return {"url": ENDPOINT, "key": API_KEY}


if __name__ == "__main__":
    main()
