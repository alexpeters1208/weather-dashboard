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
                            params={"key": KEY, "q": locations, "aqi": "yes"} | kwargs)
    else:
        payload = {"locations": [{"q": locations[i], "custom_id": f"loc{i}"} for i in range(len(locations))]}
        return requests.post(f"http://api.weatherapi.com/v1/{endpoint}.json",
                             json=payload,
                             headers={"Content-Type": "application/json"},
                             params={"key": KEY, "aqi": "yes", "q": "bulk"} | kwargs)


# Makes a request to the API endpoint specified by endpoint and produces the results to the kafka topic of the same name
def current_to_redpanda(producer, topic, batch_size, locations):

    # If we need to batch, create batches
    if not isinstance(locations, str):
        if len(locations) > batch_size:
            batch_locs = create_batch_locations(locations, batch_size)

            # Iterate through all data in each batch, produce innermost json to Kafka to maintain 1-1
            for batch_num in range(len(batch_locs)):

                batch_response = make_request("current", batch_locs[batch_num]).json()
                for response in batch_response['bulk']:

                    data = response['query']['location'] | \
                           response['query']['current'] | \
                           response['query']['current']['air_quality']
                    producer.produce(topic=topic, key=f"batch{batch_num}",
                                     value=json.dumps(data).encode('utf-8'))
                    producer.flush()

        else:
            response = make_request("current", locations).json()
            data = response['bulk']['query']['location'] | \
                   response['bulk']['query']['current'] | \
                   response['bulk']['query']['current']['air_quality']
            producer.produce(topic=topic, key=f"batch0", value=json.dumps(data).encode('utf-8'))
            producer.flush()

    else:
        response = make_request("current", locations).json()
        data = response['location'] | \
               response['current'] | \
               response['current']['air_quality']
        producer.produce(topic=topic, key=f"batch0", value=json.dumps(data).encode('utf-8'))
        producer.flush()

    return None


def historical_to_redpanda(producer, topic, batch_size, locations, start_date, end_date):

    # divide dates into bins of 30 days if necessary, historical API requires start and end to be at most 30 days apart
    date_bins = []
    for bin_num in range(((end_date - start_date).days // 30)):
        if start_date + (bin_num + 1) * datetime.timedelta(days=30) >= end_date:
            date_bins.append((start_date + bin_num * datetime.timedelta(days=30), end_date))
        else:
            date_bins.append((start_date + bin_num * datetime.timedelta(days=30),
                              start_date + (bin_num + 1) * datetime.timedelta(days=30) - datetime.timedelta(days=1)))

    # If we need to batch, create batches
    if not isinstance(locations, str):
        if len(locations) > batch_size:
            batch_locs = create_batch_locations(locations, batch_size)

            for date_range in date_bins:
                # Iterate through all data in each batch, produce innermost hour to Kafka to maintain 1-1
                for batch_num in range(len(batch_locs)):

                    batch_response = make_request(endpoint="history", locations=batch_locs[batch_num],
                                                  dt=str(date_range[0]), end_dt=str(date_range[1])).json()
                    for response in batch_response['bulk']:
                        for day in response['query']['forecast']['forecastday']:
                            for hour in day['hour']:

                                data = response['query']['location'] | hour
                                producer.produce(topic=topic, key=f"batch{batch_num}",
                                                 value=json.dumps(data).encode('utf-8'))
                                producer.flush()

        else:
            for date_range in date_bins:
                response = make_request(endpoint="history", locations=locations,
                                        dt=str(date_range[0]), end_dt=str(date_range[1])).json()

                # Produce innermost hour to Kafka to maintain 1-1
                for day in response['bulk']['query']['forecast']['forecastday']:
                    for hour in day['hour']:
                        data = response['bulk']['query']['location'] | hour
                        producer.produce(topic=topic, key=f"batch0",
                                         value=json.dumps(data).encode('utf-8'))
                        producer.flush()

    else:
        for date_range in date_bins:
            response = make_request(endpoint="history", locations=locations,
                                    dt=str(date_range[0]), end_dt=str(date_range[1])).json()

            # Produce innermost hour to Kafka to maintain 1-1
            for day in response['forecast']['forecastday']:
                for hour in day['hour']:
                    data = response['location'] | hour
                    producer.produce(topic=topic, key=f"batch0",
                                     value=json.dumps(data).encode('utf-8'))
                    producer.flush()

    return None


if __name__ == "__main__":
    # Get API key
    load_dotenv()
    KEY = os.getenv("API_KEY") or ""

    CURRENT_LOCATION = "78744"

    # Create list of locations from csv lat-long file to use in requests
    texas_coords = []
    with open("lat-long.csv", 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            texas_coords.append(f"{row['lat']},{row['long']}")

    # Get current date info to bound historical API calls
    start_historical = datetime.date.today() - datetime.timedelta(days=365)
    end_historical = datetime.date.today() - datetime.timedelta(days=1)

    # Create kafka producer
    kafka_producer = Producer({'bootstrap.servers': 'redpanda:9092'})
    #kafka_producer=True

    # Produce first batch of data
    current_to_redpanda(producer=kafka_producer,
                        topic="current-here",
                        batch_size=50,
                        locations=CURRENT_LOCATION)
    current_to_redpanda(producer=kafka_producer,
                        topic="current",
                        batch_size=50,
                        locations=texas_coords)
    historical_to_redpanda(producer=kafka_producer,
                           topic="history-here",
                           batch_size=10,
                           locations=CURRENT_LOCATION,
                           start_date=start_historical,
                           end_date=end_historical)
    historical_to_redpanda(producer=kafka_producer,
                           batch_size=10,
                           topic="history",
                           locations=texas_coords,
                           start_date=start_historical,
                           end_date=end_historical)

    # Schedule calls to the realtime api every 5 minutes, and to the forecast api every 2 hours
    schedule.every(5).minutes.do(
        current_to_redpanda,
        producer=kafka_producer,
        topic="current-here",
        batch_size=50,
        locations=CURRENT_LOCATION
    )
    schedule.every(5).minutes.do(
        current_to_redpanda,
        producer=kafka_producer,
        topic="current",
        batch_size=50,
        locations=texas_coords
    )
    schedule.every(2).hours.do(
        historical_to_redpanda,
        producer=kafka_producer,
        topic="history-here",
        batch_size=10,
        locations=CURRENT_LOCATION,
        start_date=start_historical,
        end_date=end_historical
    )
    schedule.every(2).hours.do(
        historical_to_redpanda,
        producer=kafka_producer,
        topic="history",
        batch_size=10,
        locations=texas_coords,
        start_date=start_historical,
        end_date=end_historical
    )

    while True:
        schedule.run_pending()
        time.sleep(60)
