import os
import time
import requests
import json


class DataCollector:
    """ The overarching object for the data collector.
    """

    def __init__(self, AUTH, file_path: str, max_storage: int):
        """ Passed the auth login for the API. Initialises the live records.
        :param AUTH: Authorisation dictionary
        """
        self.AUTH = AUTH
        self.records = {}
        self.save_path = file_path
        self.max_storage = max_storage
        self.request_count = 0
        self.complete = 0

    def run_collection(self, run_time: int):
        """ Runs the collection algorithm for the specified time (in seconds). Requests will be stopped for a minimum
        delay which _should_ be low enough to capture each stop.
        :param run_time: Length of time to run collection for in seconds
        """
        delay = 20
        start_time = time.perf_counter()
        end_time = start_time + run_time
        # Run loop, catch assertion errors from check_storage
        try:
            while time.perf_counter() < end_time:
                # Get the entities and initialise set for id storage
                entities = self.get_trip_updates()
                seen_ids = set()
                self.request_count += 1
                loop_start = time.perf_counter()

                # Process the trip_updates
                for entity in entities:
                    e_id = entity['trip_update']['trip']['trip_id']
                    if self.records.get(e_id) is None:
                        self.records[e_id] = TripRecord(entity)
                    else:
                        self.records[e_id].update(entity)
                    seen_ids.add(e_id)

                # Remove unseen trips as complete
                for tr_id in set(self.records.keys()) - seen_ids:
                    assert (self.check_storage())
                    self.records.pop(tr_id).export(self.save_path)
                    self.complete += 1

                # Run the delay
                while time.perf_counter() < loop_start + delay:
                    pass

                print(f"CURRENTLY:" +
                      f"\tDuration {time.perf_counter() - start_time:.1f}s out of {run_time}s" +
                      f"\tOngoing trips: {len(self.records)}" +
                      f"\tSaved trips: {self.complete}" +
                      f"\tRequests: {self.request_count}")

            # Save remaining partial trips
            for tr_id in set(self.records.keys()):
                assert (self.check_storage())
                self.records.pop(tr_id).export(self.save_path)
                self.complete += 1
            # Print final message
            print(f"END:"
                  f"\tSaved {self.complete} trips from {self.request_count} requests over "
                  f"{time.perf_counter() - start_time:.1f} seconds")
        except AssertionError:
            print(f"END: Max Storage reached with {self.complete} trips saved from {self.request_count} requests over "
                  f"{time.perf_counter() - start_time:.1f} seconds")

    def get_trip_updates(self):
        """ Collects and returns the list of current trip statuses from Metlink API
        :return entities: List of trip_updates (JSON format)
        """
        link = "/gtfs-rt/tripupdates"
        response = self.metlink_get(link)
        # If we got error on the request then don't crash, just return an empty list.
        if response is None:
            return []
        # Pull the trip_update list
        entities = response.json()["entity"]
        return entities

    def metlink_get(self, url):
        """ Sends the request to the given URL. All parameters are part of the url so the rest is constant.
        :param url: url to be added to the base
        :return r: API response
        """
        header = {
            "accept": "application/json",
            "x-api-key": self.AUTH['key']
        }
        r = requests.get(self.AUTH['url'] + url, headers=header)

        # Check it succeeded
        if r.status_code != 200:
            print(f"Requesting {url} failed with code {r.status_code}")
            return None
        return r

    def calc_storage(self):
        """ Calculates the storage space taken up in the save_path.
        :return total_size: storage space in save_path
        """
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(self.save_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
        return total_size

    def check_storage(self):
        """ Returns if max_storage has been exceeded
        :return bool: If max_storage exceeded in save_path
        """
        return self.calc_storage() < self.max_storage


class TripRecord:
    """ Storage for an ongoing trip. Once it is done it will be exported to json file.
        Note that the paths are a bit wordy TODO glom?
    """

    def __init__(self, json_obj: dict):
        """ Pulls the necessary info from the json dict. Constants over the trip and time updated are stored
        separately.
        :param json_obj: input trip_update to create the trip """
        self.id = json_obj['trip_update']['trip']['trip_id']
        self.last_updated_stop = json_obj['trip_update']['stop_time_update']['stop_id']
        self.trip_consts = json_obj['trip_update']['trip'].copy()
        # This constant is stored elsewhere.
        self.trip_consts['vehicle'] = json_obj['trip_update']['vehicle']['id']
        # This is constant across all so worthless.
        self.trip_consts.pop("schedule_relationship")
        self.trip_updates = {'arrival_time': [json_obj['trip_update']['stop_time_update']['arrival']['time']],
                             'delay': [json_obj['trip_update']['stop_time_update']['arrival']['delay']],
                             'stop_id': [json_obj['trip_update']['stop_time_update']['stop_id']],
                             'stop_sequence': [json_obj['trip_update']['stop_time_update']['stop_sequence']]}

    def update(self, json_obj: dict):
        """
        Updates the trip_record if stop is different from last updated.
        :param json_obj:
        """
        if json_obj['trip_update']['stop_time_update']['stop_id'] != self.last_updated_stop:
            self.last_updated_stop = json_obj['trip_update']['stop_time_update']['stop_id']
            self.trip_updates['arrival_time'].append(
                json_obj['trip_update']['stop_time_update']['arrival']['time'])
            self.trip_updates['delay'].append(
                json_obj['trip_update']['stop_time_update']['arrival']['delay'])
            self.trip_updates['stop_id'].append(
                json_obj['trip_update']['stop_time_update']['stop_id'])
            self.trip_updates['stop_sequence'].append(
                json_obj['trip_update']['stop_time_update']['stop_sequence'])

    def export(self, path_end):
        """ Exports itself to a json object and saves itself as the id and time.time() to ensure uniqueness.
        :param path_end: File path to save in
        """
        file_path = f"{os.getcwd()}\\{path_end}\\{self.id}{int(time.time())}.json"
        json_dict = {"trip_consts": self.trip_consts, "trip_updates": self.trip_updates, "end_time": time.asctime()}
        json_object = json.dumps(json_dict, indent=4)
        with open(file_path, "w") as outfile:
            outfile.write(json_object)
            outfile.close()

    def debug(self):
        """ Outputs the internal variables for debugging. """
        print(f"ID: {self.id}\nLast Updated: {self.last_updated_stop}\nConsts: {self.trip_consts}\n"
              f"Timed: {self.trip_updates}")
