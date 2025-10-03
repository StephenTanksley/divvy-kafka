import json
import time
import requests
from kafka import KafkaProducer


def produce_station_status_records(station_status_producer: KafkaProducer = None) -> None:

    station_updates = requests.get(url='https://gbfs.lyft.com/gbfs/2.3/chi/en/station_status.json')

    station_status_unpacked = station_updates.json()['data']['stations']
    if len(station_status_unpacked) > 0:
        for station in station_status_unpacked:
            station_status_producer.send(topic='station-status', value=station)
            print("Data sent to broker: ", station)


if __name__ == '__main__':
    bootstrap_servers = ['localhost:9092']

    # Register the producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        client_id='py_producer',
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    running = True
    while running:
        produce_station_status_records(station_status_producer=producer)
        time.sleep(45)
