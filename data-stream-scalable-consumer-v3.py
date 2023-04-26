import json
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Consumer, Producer, KafkaError

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

def consume_and_merge(partition):
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest',
        'partition.assignment.strategy': 'range',
        'enable.auto.commit': True
    })
    consumer.assign([partition])

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

    partitions = [str(i) for i in range(10)]

    # create a Kafka producer instance
    producer_conf = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(producer_conf)

    with ThreadPoolExecutor(max_workers=10) as executor:
        for partition in partitions:
            executor.submit(consume_and_merge, partition)
