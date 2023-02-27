import os
import json
import time
import requests
import numpy as np
import pandas as pd


class DataCollector:
    """ The overarching object for the data collector.
    """

    def __init__(self, AUTH, save_path: str, max_storage: int):
        """ Passed the auth login for the API. Initialises the live records.
        :param AUTH: Authorisation dictionary
        """
        self.AUTH = AUTH
        self.records = {}
        self.ground_trip_data = pd.read_csv("trip-stop-records.txt")
        self.batched_records = BatchTripRecord()
        self.max_batch_size = 100
        self.save_path = save_path
        self.max_storage = max_storage
        self.request_count = 0
        self.num_saved_trips = 0

    def run_collection(self, run_time: int):
        """ Runs the collection algorithm for the specified time (in seconds). Requests will be stopped for a minimum
        request_delay which _should_ be low enough to capture each stop.
        :param run_time: Length of time to run collection for in seconds
        """
        request_delay = 20
        start_time = time.perf_counter()
        end_time = start_time + run_time
        # Run loop, catch assertion errors from check_storage
        try:
            while time.perf_counter() < end_time:
                # Get the entities and initialise set for id storage
                loop_start = time.perf_counter()
                self.request_count += 1

                entities = self.get_trip_updates()
                finished_trip_ids = self.process_trip_updates(entities)
                self.process_finished_trips(finished_trip_ids)

                # Run the request_delay
                while time.perf_counter() < loop_start + request_delay:
                    pass

                print(f"CURRENTLY:" +
                      f"\tDuration {time.perf_counter() - start_time:.1f}s out of {run_time}s" +
                      f"\tOngoing trips: {len(self.records)}" +
                      f"\tSaved trips: {self.num_saved_trips}" +
                      f"\tRequests: {self.request_count}")

            # Save remaining partial trips
            for trip_id in set(self.records.keys()):
                self.update_batch_trips(self.records.pop(trip_id))
            self.save_batch_record()

            # Print final message
            print(f"END:"
                  f"\tSaved {self.num_saved_trips} trips from {self.request_count} requests over "
                  f"{time.perf_counter() - start_time:.1f} seconds")

        except AssertionError:
            print(f"END: Max Storage reached with {self.num_saved_trips} trips saved from {self.request_count} requests over "
                  f"{time.perf_counter() - start_time:.1f} seconds")

    def process_trip_updates(self, entities):
        """ Process the data pulled from the API into TripRecords.
        :param entities: The API data
        :return finished_ids: List of trip ids that are no longer running
        """
        # Process the trip_updates
        finished_ids = set()
        for entity in entities:
            entity_id = entity['trip_update']['trip']['trip_id']
            if self.records.get(entity_id) is None:
                self.records[entity_id] = TripRecord(entity,
                                                     self.ground_trip_data[self.ground_trip_data['trip_id'] == entity_id])
            else:
                self.records[entity_id].update(entity)
            finished_ids.add(entity_id)
        return finished_ids

    def process_finished_trips(self, finished_ids):
        """ Processes the removal and export of finished TripRecords.
        :param finished_ids: List of finished TripRecord ids
        """
        for finished_id in set(self.records.keys()) - finished_ids:
            self.update_batch_trips(self.records.pop(finished_id))

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

    def update_batch_trips(self, new_record):
        """ Updates the batch with the new record, saves BatchTripRecord if max size reached.
        :param new_record: Record to add to batch
        """
        if len(self.batched_records) >= self.max_batch_size:
            self.save_batch_record()
        self.batched_records.update(new_record)

    def save_batch_record(self):
        """ Saves the batched record if enough storage. """
        assert (self.check_storage())
        self.batched_records.export(self.save_path)
        self.num_saved_trips += len(self.batched_records)
        self.batched_records = BatchTripRecord()

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
    """

    def __init__(self, json_obj: dict, ground_data):
        """ Pulls the necessary info from the json dict. Constants over the trip and time updated are stored
        separately.
        :param json_obj: input trip_update to create the trip """
        self.id = json_obj['trip_update']['trip']['trip_id']
        self.last_updated_stop = int(json_obj['trip_update']['stop_time_update']['stop_sequence'])
        self.trip_consts = json_obj['trip_update']['trip'].copy()
        # This constant is stored elsewhere.
        self.trip_consts['vehicle'] = json_obj['trip_update']['vehicle']['id']
        # This is constant across all so worthless.
        self.trip_consts.pop("schedule_relationship")
        self.trip_updates = {'arrival_time': [json_obj['trip_update']['stop_time_update']['arrival']['time']],
                             'delay': [json_obj['trip_update']['stop_time_update']['arrival']['delay']],
                             'stop_id': [json_obj['trip_update']['stop_time_update']['stop_id']],
                             'stop_sequence': [json_obj['trip_update']['stop_time_update']['stop_sequence']]}
        # Load the ground truth trip info
        self.ground_trip_stop_id = list(ground_data["stop_id"])

    def update(self, json_obj: dict):
        """
        Updates the trip_record if stop is different from last updated.
        :param json_obj:
        """
        # if json_obj['trip_update']['stop_time_update']['stop_id'] != self.last_updated_stop:
        #     self.last_updated_stop = json_obj['trip_update']['stop_time_update']['stop_id']
        #     self.trip_updates['arrival_time'].append(
        #         json_obj['trip_update']['stop_time_update']['arrival']['time'])  # TODO: Fix the int time -> time.
        #     self.trip_updates['delay'].append(
        #         json_obj['trip_update']['stop_time_update']['arrival']['delay'])
        #     self.trip_updates['stop_id'].append(
        #         json_obj['trip_update']['stop_time_update']['stop_id'])
        #     self.trip_updates['stop_sequence'].append(
        #         json_obj['trip_update']['stop_time_update']['stop_sequence'])
        current_updated_stop = int(json_obj['trip_update']['stop_time_update']['stop_sequence'])
        stops_since_last_updated = current_updated_stop - self.last_updated_stop
        if stops_since_last_updated > 0:
            interp_locations = np.arange(1, stops_since_last_updated + 1)
            self.trip_updates['arrival_time'] += list(np.interp(interp_locations,
                                                                [0, stops_since_last_updated],
                                                                [self.trip_updates['arrival_time'][-1],
                                                                 json_obj['trip_update']['stop_time_update']['arrival'][
                                                                     'time']]))
            self.trip_updates['delay'] += list(np.interp(interp_locations,
                                                         [0, stops_since_last_updated],
                                                         [self.trip_updates['delay'][-1],
                                                          json_obj['trip_update']['stop_time_update']['arrival'][
                                                              'delay']]))
            self.trip_updates['stop_id'] += self.ground_trip_stop_id[
                                            self.last_updated_stop + 1:self.last_updated_stop + stops_since_last_updated + 1]
            self.trip_updates['stop_sequence'] += list(np.asarray(interp_locations, dtype='float64') + self.last_updated_stop)
            self.last_updated_stop = current_updated_stop

    def export(self, path_end):
        """ Exports itself to a json object and saves itself as the id and time.time() to ensure uniqueness.
        :param path_end: File path to save in
        """
        file_path = f"{os.getcwd()}\\{path_end}\\{self.id}{int(time.time())}.json"
        json_object = json.dumps(self.as_json(), indent=4)
        with open(file_path, "w") as outfile:
            outfile.write(json_object)
            outfile.close()

    def as_json(self):
        """ Returns the TripRecord in json format. """
        return {"trip_consts": self.trip_consts, "trip_updates": self.trip_updates, "end_time": time.asctime()}

    def debug(self):
        """ Outputs the internal variables for debugging. """
        print(f"ID: {self.id}\nLast Updated: {self.last_updated_stop}\nConsts: {self.trip_consts}\n"
              f"Timed: {self.trip_updates}")


class BatchTripRecord:
    """ Storage for a number of finished trips.
    """

    def __init__(self):
        """ Stores a list of trips. """
        self.trip_json = dict()

    def update(self, new_trip: TripRecord):
        """ Adds a new trip record to the batch.
        Checks for already existing id, and adds time if so.
        :param new_trip: Trip record to add
        """
        if self.trip_json.get(new_trip) is None:
            self.trip_json[new_trip.id] = new_trip.as_json()
        else:
            self.trip_json[f"{new_trip.id}{int(time.time())}"] = new_trip

    def export(self, path_end):
        """ Exports itself to a json object and saves itself as time.time() to ensure uniqueness.
        Doesn't save if empty.
        :param path_end: Folder path to save in
        """
        if len(self) == 0:
            return
        file_path = f"{os.getcwd()}\\{path_end}\\BatchedRecord-{int(time.time())}.json"
        json_object = json.dumps(self.trip_json, indent=4)
        with open(file_path, "w") as outfile:
            outfile.write(json_object)
            outfile.close()

    def __len__(self):
        """ Length of the batched record is how many trips it has. """
        return len(self.trip_json)
