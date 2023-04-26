import json
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Process
from confluent_kafka import Consumer,Producer, KafkaError

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

def consume_and_merge(bootstrap_servers, input_topic, output_topic):
    consumer_conf = {'bootstrap.servers': bootstrap_servers,
                     'group.id': 'my-group',
                     'auto.offset.reset': 'earliest',
                     'enable.auto.commit': True}
    consumer = Consumer(consumer_conf)
    consumer.subscribe([input_topic])

    producer_conf = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(producer_conf)

    partitions = [Partition() for _ in range(10)]

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

        message = json.loads(msg.value())
        segment_id = message['segment_id']
        data = message['data']

        partition = partitions[(segment_id - 1) % 10]
        for num in data:
            partition.insert(num)

        # check if all partitions have data
        if all(part.buffer for part in partitions):
            # merge all partitions
            merged = []
            for partition in partitions:
                merged += partition.buffer
            # sort the new incoming numbers with respect to the maximum number of each partition
            merged.sort(key=lambda x: (x % 10, x))
            # write to output topic
            producer.produce(output_topic, key='merged', value=json.dumps(merged).encode())
            # print merged data to console
            print('Merged data:', merged)

        producer.poll(0)


if __name__ == '__main__':
    bootstrap_servers = 'localhost:9092'
    input_topic = 'input-topic'
    output_topic = 'output-topic'
    
    num_processes = 10
    processes = []
    for i in range(num_processes):
        p = multiprocessing.Process(target=consume_and_merge, args=(bootstrap_servers, input_topic, output_topic))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()
