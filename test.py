import argparse
from Collector import DataCollector


def main(sys_args):
    AUTH = get_api_params()
    # Testing
    collector = DataCollector(AUTH, sys_args.save_path, sys_args.max_storage)
    collector.run_collection(sys_args.run_time)


def get_api_params():
    """ Pulls the Endpoint and Authentication key from key.txt. These are laid out in consecutive lines.
    :return: {url: _, key: _}"""
    file = open("key.txt", 'r')
    ENDPOINT = file.readline()[:-1]  # Remove new line char
    API_KEY = file.readline()[:-1]
    return {"url": ENDPOINT, "key": API_KEY}


if __name__ == "__main__":
    """ Command Line Parse and check. 
        CMD params:
        :param run_time: Time to run_program for (seconds)
        :param save_path: Save path for trips
        :param max_storage: Max amount of space to use in save_path
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("run_time", help="Length of time (seconds) to run the program", type=int)
    parser.add_argument("-sp", "--save_path", help="Path to save collected trips to", type=str, default="SavedTrips")
    parser.add_argument("-ms", "--max_storage", help="Maximum bytes of data to store in the save path", type=int,
                        default=1e9)
    args = parser.parse_args()
    main(args)
