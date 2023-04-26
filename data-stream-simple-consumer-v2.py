from confluent_kafka import Consumer, KafkaError, Producer
from multiprocessing import Process
import json

class Partition:
    def __init__(self):
        self.buffer = []
        self.max_num = float('-inf')

    def insert(self, num):
        if num < self.max_num:
            return False
        self.buffer.append(num)
        self.max_num = num
        return True

# Create a Kafka consumer configuration
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True}

# Create a Kafka consumer instance
consumer = Consumer(conf)
consumer.subscribe(['input-topic'])

# Create partitions
partitions = [Partition() for _ in range(10)]
current_partition = 0

# Create a Kafka producer configuration
producer_conf = {'bootstrap.servers': 'localhost:9092'}

# Create a Kafka producer instance
producer = Producer(producer_conf)

# Consume messages from all partitions
while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('Reached end of partition')
        else:
            print('Error while consuming message: {}'.format(msg.error()))
        continue

    # Deserialize the message from JSON
    message = json.loads(msg.value())

    # Get the data from the message
    data = message

    # Insert the data into the appropriate partition
    partition = partitions[current_partition]
    for num in data:
        partition.insert(num)
        current_partition = (current_partition + 1) % 10

    # Check if all partitions have data
    if all(part.buffer for part in partitions):
        # Merge all partitions
        merged = []
        for partition in partitions:
            merged += partition.buffer
        # Sort the merged partition
        merged.sort()
        # Write to output topic
        producer.produce('output-topic', key='merged', value=' '.join(map(str, merged)))
        
        # Print merged data to console
        print('Merged data:', merged)
        
        producer.flush()
