import time
import requests


class DataCollector:
    """ The overarching object for the data collector.
    """

    def __init__(self, AUTH):
        """ Passed the auth login for the API. Initialises the live records.
        :param AUTH: Authorisation dictionary
        """
        self.AUTH = AUTH
        self.records = {}
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
        # Run loop
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
                self.records.pop(tr_id).export()
                self.complete += 1

            # Run the delay
            while time.perf_counter() < loop_start + delay:
                pass
            print(f"\rDuration {time.perf_counter() - start_time:.1f}s out of {run_time}s"
                  f"\tCurrently running trips: {len(self.records)}"
                  f"\tSaved trips: {self.complete}"
                  f"\tRequests: {self.request_count}", end="")

        # Save remaining partial trips
        for tr_id in set(self.records.keys()):
            self.records.pop(tr_id).export()
            self.complete += 1

        # Print final message
        print(f"\nSaved {self.complete} trips from {self.request_count} requests over "
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
        self.trip_consts['vehicle'] = json_obj['trip_update']['vehicle']['id']
        self.trip_consts.pop("schedule_relationship")
        self.trip_updates = {'arrival_time': [json_obj['trip_update']['stop_time_update']['arrival']['time']],
                             'delay': [json_obj['trip_update']['stop_time_update']['arrival']['delay']],
                             'stop_id': [json_obj['trip_update']['stop_time_update']['stop_id']],
                             'stop_sequence': [json_obj['trip_update']['stop_time_update']['stop_sequence']]}

    def update(self, json_obj: dict):
        """
        Updates the trip_record if timestamp different from last updated.
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

    def export(self):
        """ Exports itself to a json object and saves itself as the id. """
        # TODO: save itself as JSON file.
        pass

    def debug(self):
        """ Outputs the internal variables for debugging. """
        print(f"ID: {self.id}\nLast Updated: {self.last_updated_stop}\nConsts: {self.trip_consts}\n"
              f"Timed: {self.trip_updates}")
