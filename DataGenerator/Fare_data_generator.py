import time
import os
from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.exceptions import EventHubError
import json
import pandas as pd



CONNECTION_STR = ''
EVENTHUB_NAME = 'second'


# [START send_event_data_batch]
def send_event_data_batch(producer):
    # Without specifying partition_id or partition_key
    # the events will be distributed to available partitions via round-robin.
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData('Single message'))
    producer.send_batch(event_data_batch)
# [END send_event_data_batch]


def send_event_data_batch_with_limited_size(producer):
    # Without specifying partition_id or partition_key
    # the events will be distributed to available partitions via round-robin.
    event_data_batch_with_limited_size = producer.create_batch(max_size_in_bytes=1000)

    while True:
        try:
            event_data_batch_with_limited_size.add(EventData('Message inside EventBatchData'))
        except ValueError:
            # EventDataBatch object reaches max_size.
            # New EventDataBatch object can be created here to send more data.
            break

    producer.send_batch(event_data_batch_with_limited_size)


def send_event_data_batch_with_partition_key(producer):
    # Specifying partition_key.
    event_data_batch_with_partition_key = producer.create_batch(partition_key='pkey')
    event_data_batch_with_partition_key.add(EventData('Message will be sent to a partition determined by the partition key'))

    producer.send_batch(event_data_batch_with_partition_key)


def send_event_data_batch_with_partition_id(producer,json_data,partition_ids):

    
    serialized_data = json.dumps(json_data).encode('utf-8')
    # Specifying partition_id.
    event_data_batch_with_partition_id = producer.create_batch(partition_id=partition_ids)
    event_data_batch_with_partition_id.add(EventData(serialized_data))

    producer.send_batch(event_data_batch_with_partition_id)


def send_event_data_batch_with_properties(producer):
    event_data_batch = producer.create_batch()
    event_data = EventData('Message with properties')
    event_data.properties = {'prop_key': 'prop_value'}
    event_data_batch.add(event_data)
    producer.send_batch(event_data_batch)


def send_event_data_list(producer):
    # If you know beforehand that the list of events you have will not exceed the
    # size limits, you can use the `send_batch()` api directly without creating an EventDataBatch

    # Without specifying partition_id or partition_key
    # the events will be distributed to available partitions via round-robin.

    event_data_list = [EventData('Event Data {}'.format(i)) for i in range(10)]
    try:
        producer.send_batch(event_data_list)
    except ValueError:  # Size exceeds limit. This shouldn't happen if you make sure before hand.
        print("Size of the event data list exceeds the size limit of a single send")
    except EventHubError as eh_err:
        print("Sending error: ", eh_err)


producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name=EVENTHUB_NAME
)




# START OF THE PROGRAM

print("PROGRAM STARTED")
# READING fare_DATA

df_fare= pd.read_csv('eventDataAzure/trip_fare_1.csv')

#df.iloc[49:100]
partition_id="0"
for index, row_data in df_fare.iloc[500:100000].iterrows():


    fare_data={
            "medallion": row_data['medallion'],
            "hack_license":row_data[' hack_license'],
            "vendor_id": row_data[' vendor_id'],
            "pickup_datetime": row_data[' pickup_datetime'],
            "payment_type": row_data[' payment_type'],
            "fare_amount": row_data[' fare_amount'],
            "surcharge": row_data[' surcharge'],
            "mta_tax": row_data[' mta_tax'],
            "tip_amount": row_data[' tip_amount'],
            "tolls_amount": row_data[' tolls_amount'],
            "total_amount": row_data[' total_amount']
        }
    
    
    start_time = time.time()
    with producer:
        #send_event_data_batch(producer)
        #send_event_data_batch_with_limited_size(producer)
        #send_event_data_batch_with_partition_key(producer)
        send_event_data_batch_with_partition_id(producer,fare_data,partition_id)
        #send_event_data_batch_with_properties(producer)
        #send_event_data_list(producer)

    print("Send messages in {} seconds.".format(time.time() - start_time))

    if partition_id=="1":
        partition_id="0"
    else:
        partition_id="1"