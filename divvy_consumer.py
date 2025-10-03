import json
import time

from kafka import KafkaConsumer, TopicPartition  # Technically TopicPartition is not a public interface. I'm aware.
from pandas import DataFrame
import pandas as pd
from pandas import Series


def get_last_offset_from_topic(topic: str = "", consumer: KafkaConsumer = None):
    partition = TopicPartition(topic, 0)
    # TODO: This could become a problem in the future since I'm only looking at a single partition.
    #  This is easy to fix as long as I'm mapping ALL partitions and getting the offsets for all partitions.
    #  For now, this is fine since I've only got the one partition now and I'll only have the one partition.
    end_offsets = consumer.end_offsets([partition])
    last_offset = list(end_offsets.values())[-1]

    return last_offset


def get_station_info(path:str = ""):
    if path == "":
        raise ValueError("Please provide a valid path for the enrichment file")

    with open('./data/station_info.json', 'r') as json_file:
        data = json.load(json_file)['data']['stations']

    return DataFrame([item for item in data])

def get_station_status(consumer: KafkaConsumer=None):
    station_status = []

    status_records = consumer.poll(
        update_offsets=True,
        max_records=2000,
        timeout_ms=1000
    )

    consumer_items = [item for item in status_records.values()]

    for item in consumer_items:
        for inner_item in item:
            station_status.append(inner_item.value)
    return DataFrame(station_status)


def impute_zero(x: int | str | None):
    if x is None or x == 'null':
        value = 0
    else:
        value = x
    return value


def enrich_with_cap_ratio(num_bikes_available: pd.Series=None, capacity: pd.Series=None):
    collection = []
    for tup in zip(num_bikes_available, capacity):
        if tup[-1] == 0:
            collection.append(0)
        else:
            collection.append(tup[0] / tup[-1])

    return collection


def enrich_with_color(df_col: Series):
    colors = []

    for item in df_col:
        if item <= .33:
            colors.append('#0d466c')
        elif .34 <= item <= .67:
            colors.append('#298538')
        else:
            colors.append('#d61437')

    return colors


if __name__ == '__main__':
    # Set up the consumer we'll need.
    bootstrap_servers = ['localhost:9092']
    station_status_consumer = KafkaConsumer(
        'station-status',
        client_id='py_consumer',
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=True,
        group_id="station_status",
        session_timeout_ms=60000, # setting a slightly longer timeout here because we're not strictly streaming - more microbatch
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )


    # Comparing offset on station_status because this is dynamically updating.
    station_status_last_offset = 0

    running = True
    while running:
        station_status_current_offset = get_last_offset_from_topic(topic='station-status', consumer=station_status_consumer)

        try:
            if station_status_current_offset > station_status_last_offset:
                station_status_df = get_station_status(consumer=station_status_consumer)
                station_info_df = get_station_info(path='./data/station_info.json')

                print(f"Fast-forwarding to current offset : {station_status_last_offset=}, {station_status_current_offset=}")
                if 'station_id' in station_status_df.columns and 'station_id' in station_info_df.columns:
                    df = pd.merge(station_status_df, station_info_df, how='inner', on='station_id')

                    # Enrichment logic
                    df['num_bikes_available'] = df['num_bikes_available'].apply(impute_zero)
                    df['cap_ratio'] = enrich_with_cap_ratio(num_bikes_available=df.get('num_bikes_available'), capacity=df.get('capacity'))
                    df['color'] = enrich_with_color(df['cap_ratio'])

                    # TODO: Eventually, I'll want to append a timestamp to this so we can compare station usage across timestamps.
                    #  This will produce a new file every time we update.
                    with open('./data/station_status_updated.json', 'w') as file:
                        file.write(df.to_json(orient='records'))

                station_status_last_offset = station_status_current_offset
                time.sleep(30)
            else:
                print("Current dataframe is up to date")
        except Exception as e:
            print("Something went HORRIBLY wrong: ", str(e))
