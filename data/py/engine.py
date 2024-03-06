import os
import csv
import time
import json
import requests
import schedule
import datetime
from typing import Iterable
from dotenv import load_dotenv
from confluent_kafka import Producer


# Split list of locations up into list of lists of locations, useful for navigating API restrictions on batch requests
def create_batch_locations(locations, batch_size):
    batches = []
    num_batches = ((len(locations) - 1) // batch_size) + 1 if not isinstance(locations, str) else 1
    for batch_num in range(num_batches):
        if ((batch_num + 1) * batch_size) > len(locations):
            batches.append(locations[batch_num * batch_size:])
        else:
            batches.append(locations[batch_num * batch_size: (batch_num + 1) * batch_size])

    return batches


def make_request(endpoint: str, locations: str | Iterable[str], **kwargs):
    if isinstance(locations, str):
        return requests.get(f"http://api.weatherapi.com/v1/{endpoint}.json",
                            headers={"Content-Type": "application/json"},
                            params={"key": KEY, "q": locations} | kwargs)
    else:
        payload = {"locations": [{"q": locations[i], "custom_id": f"loc{i}"} for i in range(len(locations))]}
        return requests.post(f"http://api.weatherapi.com/v1/{endpoint}.json",
                             json=payload,
                             headers={"Content-Type": "application/json"},
                             params={"key": KEY, "q": "bulk"} | kwargs)


# Makes a request to the API endpoint specified by endpoint and produces the results to the kafka topic of the same name
def produce_to_redpanda(endpoint, locations, producer, batch_size, **kwargs):

    # If we need to batch, create them and produce to redpanda in a loop
    if not isinstance(locations, str) and len(locations) > batch_size:
        batch_locs = create_batch_locations(locations, batch_size)

        for i in range(len(batch_locs)):
            batch_response = make_request(endpoint, batch_locs[i], **kwargs)
            producer.produce(topic=endpoint, key=f"batch{i}", value=json.dumps(batch_response.json()).encode('utf-8'))

    else:
        response = make_request(endpoint, locations, **kwargs)
        producer.produce(topic=endpoint, key="batch0", value=json.dumps(response.json()).encode('utf-8'))

    producer.flush()
    return None


if __name__ == "__main__":
    # Get API key
    load_dotenv()
    KEY = os.getenv("API_KEY") or ""

    # Create list of locations from csv lat-long file to use in requests
    texas_coords = []
    with open("lat-long.csv", 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            texas_coords.append(f"{row['lat']},{row['long']}")

    # Get current date info to bound historical API calls
    end_historical = datetime.datetime.today() - datetime.timedelta(days=1)
    start_historical = end_historical - datetime.timedelta(days=1)

    # Create kafka producer
    kafka_producer = Producer({'bootstrap.servers': 'redpanda:9092'})

    # Produce first batch of data
    produce_to_redpanda(endpoint="current",
                        locations=texas_coords,
                        producer=kafka_producer,
                        batch_size=50)
    produce_to_redpanda(endpoint="history",
                        locations=texas_coords,
                        producer=kafka_producer,
                        batch_size=10,
                        dt=str(start_historical),
                        end_dt=str(end_historical))

    # Schedule calls to the realtime api every 5 minutes, and to the forecast api every 2 hours
    schedule.every(5).minutes.do(
        produce_to_redpanda,
        endpoint="current",
        locations=texas_coords,
        producer=kafka_producer,
        batch_size=50
    )
    schedule.every(2).hours.do(
        produce_to_redpanda,
        endpoint="history",
        locations=texas_coords,
        producer=kafka_producer,
        batch_size=10,
        dt=str(start_historical),
        end_dt=str(end_historical)
    )

    while True:
        schedule.run_pending()
        time.sleep(60)
