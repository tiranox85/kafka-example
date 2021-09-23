# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer

# Import sys module
import sys

# Define server with port
bootstrap_servers = ['localhost:9091', 'localhost:9092', 'localhost:9093']

# Define topic name from where the message will recieve
topicName = 'credijusto-test'

# Initialize consumer variable
consumer = KafkaConsumer(
    topicName,
    group_id='group1',
    bootstrap_servers=bootstrap_servers,
    enable_auto_commit=False,
    auto_offset_reset="earliest",
    max_poll_records=100
)
consumer.poll()
# go to end of the stream
consumer.seek_to_end()

# Read and print message from consumer
for msg in consumer:
    print(msg.value)
    consumer.commit()

# Terminate the script
sys.exit()
