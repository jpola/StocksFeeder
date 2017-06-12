from azure.servicebus import ServiceBusService, Message, Queue
from azure.common import AzureHttpError
import json as js
import sys


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


class AzureConnector:

    def __init__(self, service_name, key_name, key_value):
        self.key_name = key_name
        self.key_value = key_value
        self.service_name = service_name
        self.records = []
        self.limit = 256 * 1024 # Message limit for eventHub
        self.sbs = ServiceBusService(service_name,
                                     shared_access_key_name=key_name,
                                     shared_access_key_value=key_value)

    def to_list(self, df, ticker):
        '''
        generate list of dictionaries 'key':val from dataframe
        Later it is easier to:
            - save to file (json)
            - send to Azure Event HUB
        in appropriate format.

        The format is:
        { ticker : 'MSFT', timestamp:'2015-01-01', high:'100', low:'50', close:'75', volume:'10', ....(rest of tech. indicators)}

        :param df: dataframe to process
        :param ticker: name of the dataframe ticker i.e. "MSFT"
        :return:
        '''
        if self.records:
            self.records = []

        for i in range(df.shape[0]):
            rec = {'ticker': ticker, 'date': str(df.index[i])}
            rec.update(df.iloc[i])
            self.records.append(rec)


    def to_file(self, df, ticker, filename):
        '''

        Save data to file.

        :param df: dataframe to save
        :param ticker: ticker of the dataframe
        :param filename: name of the file
        :return:
        '''
        if not self.records:
            self.to_list(df, ticker)
        with open(filename, 'w') as f:
            js.dump(self.records, f, indent=2)

    def send_event(self, hub_name, message):
        try:
            self.sbs.send_event(hub_name, message)
        except AzureHttpError as ahe:
            print("Error during send_event: ", ahe)


    def send(self, hub_name, df, ticker):
        self.to_list(df, ticker)
        self.check_size()
        _chunks = list(chunks(self.records, self.n_records_per_chunk))
        _csize = len(_chunks)

        index = 1
        for c in _chunks:
            print("Sending: %d / %d chunk with %d entries" % (index, _csize, len(c)))
            msg = js.dumps(c)
            self.send_event(hub_name, msg)
            index += 1


    def check_size(self):
        '''
        Check the size (bytes) of the data to send.
        EventHub has 256Kb limit per message.

        If the data is larger it will be divided into chunks.

        :return: size of the data to send
        '''

        self.size = 0
        _count = len(self.records)
        index = 0
        for r in self.records:
            entry = sys.getsizeof(js.dumps(r, indent=2))
            # Measuring last record.
            # Most probably the last one have most non nan vals which are longer when stringified
            if index == len(self.records) - 1:
                lsize = entry
            self.size += entry
            index += 1

        self.n_records_per_chunk = round(self.limit / lsize)

        return self.size

