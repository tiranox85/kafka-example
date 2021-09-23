import json

# Import KafkaConsumer from Kafka library
from kafka import KafkaProducer

# Define server with port
bootstrap_servers = ['localhost:9091', 'localhost:9092', 'localhost:9093']

# Define topic name from where the message will recieve
topicName = 'credijusto-test'

# Set a json message
json_string = {"message": "Hello kafka fer"}
message = json.dumps(json_string)

# Create the producer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
producer = KafkaProducer()

# Produce the message
producer.send(topicName, key=b'loan-678784', value=json.dumps(json_string).encode('utf-8'))
producer.flush()
