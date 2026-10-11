"Polls LTA DataMall API for traffic speed data and publishes to Kafka topic traffic_speed" 

import json 
import os 
import time
from datetime import datetime, timezone, timedelta 
import requests 
from confluent_kafka import Producer 

# ---------- Config ----------
LTA_URL = "https://datamall2.mytransport.sg/ltaodataservice/v4/TrafficSpeedBands"
API_KEY = os.environ["LTA_ACCOUNT_KEY"]
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "traffic-speed"
POLL_SECONDS = 120          # LTA updates every ~2-5 minutes, so polling faster is wasteful
SGT = timezone(timedelta(hours=8))
 
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def fetch_all_records(): 
    """
    Fetches all records from LTA DataMall API for traffic speed data at most 500 reocords per request
    Returns:
        list: List of all records fetched from the API
    """

    records, skip = [], 0 
    while True: 
        response = requests.get(
            LTA_URL, 
            headers={"AccountKey": API_KEY},
            params={"$skip": skip},
            timeout=30
        )

        response.raise_for_status()  # Raise an exception for HTTP errors
        page = response.json().get("value", [])
        if not page:  # If the page is empty, we've fetched all records
            break
        records.extend(page)
        skip += 500 
    print(f"Fetched {len(records)} records, {len({r['LinkID'] for r in records})} unique roads")
    return records

def to_event(raw, ts): 
    """
    Converts a record from LTA Mall API to a Kafka event format
    Returns:
        dict: A dictionary representing the Kafka event
    """
    return {
        "road_id": raw["LinkID"],
        "road_name": raw["RoadName"],
        "speed_band": clean(raw["SpeedBand"], int),
        "min_speed_kmh": clean(raw["MinimumSpeed"], float),
        "max_speed_kmh": clean(raw["MaximumSpeed"], float),
        "start_lat": clean(raw["StartLat"], float),
        "start_lon": clean(raw["StartLon"], float),
        "end_lat": clean(raw["EndLat"], float),
        "end_lon": clean(raw["EndLon"], float),
        "event_timestamp": ts,
    }

def on_delivery(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")

def clean(value, cast):
    """
    Cleans and casts a value, returning None if the value is None or an empty string.
    """
    if value is None or value == "":
        return None
    return cast(value)

def main(): 
    """
    Main function to fetch traffic speed data from LTA DataMall API and publish to Kafka topic
    """
    while True: 
        try: 
            records = fetch_all_records()
            ts = datetime.now(SGT).isoformat(timespec='seconds')  # Current timestamp in ISO format
            for record in records:
                try:
                    event = to_event(record, ts)
                except ValueError:
                    print("BAD RECORD:", record)
                    continue
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=str(event["road_id"]),
                    value=json.dumps(event),
                    callback=on_delivery
                )
                producer.poll(0)  # Trigger delivery report callbacks

            producer.flush()  # Wait for all messages to be delivered
            print(f"Published {len(records)} records to Kafka topic {KAFKA_TOPIC} at {ts}")
        except Exception as e: 
            print(f"An error occurred: {e}")
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()