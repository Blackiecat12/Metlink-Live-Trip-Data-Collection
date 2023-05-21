import argparse
from Collector import DataCollector


def main(sys_args):
    """ Runs the collector. """
    AUTH = get_api_params()
    collector = DataCollector(AUTH, sys_args)
    collector.run_collection(sys_args.run_time, sys_args.run_time_unit)


def get_api_params():
    """ Return Endpoint and Authentication key from key.txt.
    These are laid out in consecutive lines.
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
    parser.add_argument("run_time", help="Length of time to run the program (Default in seconds)", type=int)
    parser.add_argument("-rtu", "--run_time_unit", help="Unit to use for run time argument. Seconds [s], Minutes [m], "
                                                        "Hours [h], Days [d]", type=str, default='s',
                        choices=['s', 'm', 'h', 'd'])
    parser.add_argument("-sp", "--save_path", help="Path to save collected trips to", type=str, default="SavedTrips")
    parser.add_argument("-rd", "--request_delay", help="Minimum delay in seconds between API requests", type=int,
                        default=120)
    parser.add_argument("-ms", "--max_storage", help="Maximum bytes of data to store in the save path", type=int,
                        default=1e9)
    args = parser.parse_args()
    main(args)
