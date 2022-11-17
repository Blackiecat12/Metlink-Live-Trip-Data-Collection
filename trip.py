class trip_record:
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
        pass

    def debug(self):
        """ Outputs the internal variables for debugging. """
        print(f"ID: {self.id}\nLast Updated: {self.last_updated_stop}\nConsts: {self.trip_consts}\n"
              f"Timed: {self.trip_updates}")
