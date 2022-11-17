import json
import requests
import pandas as pd


def main():
    AUTH = get_api_params()
    test_ext = "/gtfs-rt/tripupdates"
    link = test_ext
    response = metlink_get(link, AUTH)
    df = pd.DataFrame.from_dict(response.json()['entity'])
    df.info()
    df.to_csv("test.csv", index=False)


def get_tripupdates():
    """ Currently a test function to get """

def metlink_get(url, AUTH):
    """ Sends the request to the given URL. All parameters are part of the url so the rest is constant. """
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


def get_api_params():
    """ Pulls the Endpoint and Authentication key from key.txt. These are laid out in consecutive lines. """
    file = open("key.txt", 'r')
    ENDPOINT = file.readline()[:-1]  # Remove new line char
    API_KEY = file.readline()[:-1]
    return {"url": ENDPOINT, "key": API_KEY}


if __name__ == "__main__":
    main()
