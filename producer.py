import csv
import time
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import json

# Function to convert datetime string to milliseconds since epoch
def datetime_to_millis(dt_str):
    dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    return int(time.mktime(dt.timetuple()) * 1000)

# Function to convert date string to days since epoch
def date_to_days(date_str):
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    epoch = datetime(1970, 1, 1)
    return (dt - epoch).days

# Load configuration
def read_config():
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config

# Load Avro schema
with open('vod_usr.avsc', 'r') as f:
    schema_str = f.read()

# Read configurations
config = read_config()

# Extract Schema Registry configurations
schema_registry_conf = {
    'url': config.get('schema.registry.url'),
    'basic.auth.user.info': config.get('basic.auth.user.info'),
}

# Initialize Schema Registry Client
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Define a function to convert the record to a dict (if needed)
def to_dict(record, ctx):
    return record

# Initialize AvroSerializer
avro_serializer = AvroSerializer(schema_registry_client, schema_str, to_dict)

# Initialize Producer
producer_conf = {
    'bootstrap.servers': config.get('bootstrap.servers'),
    'security.protocol': config.get('security.protocol'),
    'sasl.mechanism': config.get('sasl.mechanism'),
    'sasl.username': config.get('sasl.username'),
    'sasl.password': config.get('sasl.password')
}
producer = Producer(producer_conf)

# Read CSV and produce messages
with open('kaggle_dataset.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        try:
            value = {
                "row_id": int(row['row_id']),
                "datetime": datetime_to_millis(row['datetime']),
                "duration": float(row['duration']),
                "title": row['title'],
                "genres": row['genres'].split('|'),
                "release_date": date_to_days(row['release_date']),
                "movie_id": row['movie_id'],
                "user_id": row['user_id']
            }
            serialized_value = avro_serializer(value, SerializationContext('movie_watch_events', MessageField.VALUE))
            producer.produce(topic='movie_watch_events', value=serialized_value)
            producer.flush()
            print(f"Produced record to topic 'movie_watch_events': {value}")
        except Exception as e:
            print(f"Failed to produce record: {e}")

