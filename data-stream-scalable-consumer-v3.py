from confluent_kafka import KafkaError, KafkaException, Consumer, Producer, TopicPartition
import threading
import queue
import time
import json
from multiprocessing import Process

# Define Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

# Define Kafka producer configuration
producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}

# Define output topic name
output_topic = 'output-topic'

# Define number of partitions
num_partitions = 10

# Define buffer size and time interval
buffer_size = 10000
time_interval = 5

# Define a priority queue for each partition
queues = [queue.PriorityQueue() for _ in range(num_partitions)]

# Define a lock to synchronize access to the queues
lock = threading.Lock()

# Define a flag to indicate when all messages have been consumed
all_messages_consumed = False
msg_queue = queue.Queue()
# Define a function to read messages from a partition
def consume_partition(partition):
    global all_messages_consumed
    global msg_queue
    consumer = Consumer(conf)
    consumer.assign([TopicPartition('input-topic', partition)])
    while not all_messages_consumed:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('Reached end of partition:', partition)
                else:
                    raise KafkaException(msg.error())
                continue
            # Deserialize the message from JSON
            message = json.loads(msg.value())
            # Get the data from the message
            data = message
            # Add data to the message queue
            with lock:
                for num in data:
                    queues[num % num_partitions].put(num)
        except KafkaException as e:
            print('Error while reading partition:', partition, e)
            break
        except Exception as e:
            print('Unexpected error while reading partition:', partition, e)
            break
    consumer.close()


# Define a function to merge messages from all queues and write them to the output topic
def merge_and_write(msg_queue):
    global all_messages_consumed
    producer = Producer(producer_conf)
    buffer = []
    last_flush_time = time.time()
    while not all_messages_consumed:
        try:
            # Merge messages from all queues
            # Merge messages from all queues
            with lock:
                for q in queues:
                    while not q.empty():
                        buffer.append(q.get()[1])
            # Write merged messages to output topic if buffer is full or time interval has passed
            if len(buffer) >= buffer_size or time.time() - last_flush_time >= time_interval:
                buffer.sort()
                producer.produce(output_topic, key='merged', value=' '.join(map(str, buffer)))
                producer.flush()
                print('Merged data:', buffer)
                buffer = []
                last_flush_time = time.time()
        except Exception as e:
            print('Unexpected error while merging data:', e)
            break
    producer.flush()
    producer.close()

# Start the consumer processes
processes = [Process(target=consume_partition, args=(i,)) for i in range(num_partitions)]
for p in processes:
    p.start()

# Start the merge-and-write process

merge_process = Process(target=merge_and_write, args=(msg_queue,))
merge_process.start()

# Wait for all processes to finish
for p in processes:
    p.join()
merge_process.join()

# Set the flag to indicate that all messages have been consumed
all_messages_consumed = True
