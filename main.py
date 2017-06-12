from stocks_feeder.stocks_feeder import downloader as dl
from stocks_feeder.stocks_feeder import technical_indicators as ti
from stocks_feeder.stocks_feeder import azure_connector as ac


start_date = '2005-01-01'
# If end date is not given the default value is today
end_date = '2017-06-12'

resource_name = 'yahoo'  # 'google'

ticker = 'MSFT'

yahoo_frame = dl.get_data(resource_name, ticker, start_date, end_date)


# We can easily serialize the data using pickle mechanism
#dl.save_pckl(yahoo_frame, 'yframe.pckl')
# And load saved data
#yahoo_frame = dl.load_pckl('yframe.pckl')


# Calculate technical indicators. By default normalization is set to false.
yahoo_frame = ti.calculate_indicators(yahoo_frame)

# Set connection to Azure
key_name = 'RootManageSharedAccessKey'
key_value = '5ug8QabNX3YGNYqI+YK9V+cJwwBvXt3NFhqDaGUI8wI='
bus_service_name = 'financeInput'
hub_name = 'input'


connector = ac.AzureConnector(bus_service_name,
                              key_name, key_value)

# send dataframe to Azure EventHUB
connector.send(hub_name, yahoo_frame, ticker)
