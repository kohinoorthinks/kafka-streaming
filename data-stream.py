from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import json
import random
import time

class Partition:
    def __init__(self, id):
        self.id = id
        self.segments = []
        self.offset = 0
    
    def add_segment(self, segment):
        self.segments.append(segment)
        self.offset += 1
    
    def get_segments(self):
        return self.segments

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} to partition [{msg.partition()}] at offset {msg.offset()}')

# Create the Kafka producer configuration
conf = {'bootstrap.servers': 'localhost:9091,localhost:9092,localhost:9093'}

# Create the Kafka producer instance
producer = Producer(conf)

# Create the Kafka topic with 10 partitions
topic = 'input-topic'
num_partitions = 10

# Create the Kafka admin client instance
admin_client = AdminClient(conf)

# Delete the topic if it already exists
admin_client.delete_topics([topic])

# Wait for the topic to be deleted
time.sleep(5)

# Create the topic with 10 partitions and a replication factor of 1
topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=1)
admin_client.create_topics([topic])

# Create a list of Partition objects to represent each partition in the Kafka topic
partitions = [Partition(i) for i in range(num_partitions)]
partitions_sent = 0
while True:
    # Generate a random list of integers sorted in ascending order
    segment_length = random.randint(10, 20)
    segment = sorted([random.randint(1, 50) for _ in range(segment_length)])
    
    # Calculate the partition to send the message to using round-robin
    partition_id = partitions_sent % num_partitions
    partitions_sent += 1
    
    # Get the partition object
    partition = partitions[partition_id]
    
    # Add the segment to the partition
    partition.add_segment(segment)
    
    # Create a message for the segment
    message = {'segment_id': partition_id, 'data': segment}
    
    # Serialize the message to JSON
    serialized_message = json.dumps(message).encode('utf-8')
    
    # Send the message to the Kafka topic
    producer.produce(topic='input-topic', value=serialized_message, partition=partition.id, callback=delivery_report)
    
    # Flush the producer buffer to make sure the message is sent immediately
    producer.flush()
    
    # Wait for a random amount of time before sending the next message
    time.sleep(random.uniform(0.5, 2.0))

