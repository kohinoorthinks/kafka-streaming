from confluent_kafka import Consumer, KafkaError, Producer
from multiprocessing import Process, Queue
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

def consumer_process(queue):
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
            # Send merged partition to main process
            queue.put(merged)

# Create a Kafka producer configuration
producer_conf = {'bootstrap.servers': 'localhost:9092'}

# Create a Kafka producer instance
producer = Producer(producer_conf)

# Create queues for communication between consumer processes and main process
queues = [Queue() for _ in range(10)]

# Start consumer processes
consumer_processes = []
for i in range(10):
    p = Process(target=consumer_process, args=(queues[i],))
    consumer_processes.append(p)
    p.start()

# Merge and sort data from all partitions as it becomes available
merged_partitions = []
while True:
    # Get the next merged partition from the queue
    merged_partition = None
    for queue in queues:
        if not queue.empty():
            merged_partition = queue.get()
            break
    if merged_partition is None:
        continue

    # Add the merged partition to the list of merged partitions
    merged_partitions.append(merged_partition)

    # If all partitions have data, merge and sort all partitions
    if len(merged_partitions) == 10:
        merged = []
        for partition in merged_partitions:
            merged += partition
        merged.sort()

        # Write to output topic
        producer.produce('output-topic', key='merged', value=' '.join(map(str, merged)))

        # Print merged data to console
        print('Merged data:', merged)

        producer.flush()

        # Reset list of merged partitions
        merged_partitions = []

# Join consumer processes
for p in consumer_processes:
    p.join()
