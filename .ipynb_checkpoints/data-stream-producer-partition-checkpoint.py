from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import json
import random
import time

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} to partition [{msg.partition()}] at offset {msg.offset()}')
        with open('output/output.txt', 'a') as f:
            f.write(msg.value().decode() + '\n')

# Create the Kafka producer configuration
conf = {'bootstrap.servers': 'localhost:9091,localhost:9092,localhost:9093'}

# Create the Kafka producer instance
producer = Producer(conf)

# Create the Kafka admin client configuration
admin_conf = {'bootstrap.servers': 'localhost:9091,localhost:9092,localhost:9093'}

# Create the Kafka admin client instance
admin_client = AdminClient(admin_conf)

# Delete the topic if it already exists
admin_client.delete_topics(['segment-topic'])

# Wait for the topic to be deleted
time.sleep(5)

# Create the topic with 10 partitions and a replication factor of 1
topic = NewTopic('segment-topic', num_partitions=10, replication_factor=1)
admin_client.create_topics([topic])

batch_id = 0

while True:
    batch_id += 1
    for segment_id in range(1, 11):
        segment = sorted([random.randint(1, 10) for _ in range(random.randint(10, 20))])
        # Calculate the partition number for the segment
        partition = segment_id - 1
        # Create a Kafka message for the segment
        message = {'batch_id': batch_id,'segment_id': segment_id -1, 'data': segment}
        # Serialize the message to JSON
        serialized_message = json.dumps(message).encode('utf-8')
        # Send the message to the Kafka topic, specifying the partition number
        producer.produce(topic='segment-topic', value=serialized_message, partition=partition, callback=delivery_report)
        producer.flush()

    print(f'Batch {batch_id} produced successfully.')

    # Wait for 1 minute before producing the next batch
    time.sleep(10)

# Keep the producer running indefinitely
while True:
    pass
