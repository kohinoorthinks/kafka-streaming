from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Consumer, KafkaError, TopicPartition
import json
import heapq
import time

def consume_segment_topic(partition):
    # Create the Kafka consumer configuration
    conf = {'bootstrap.servers': 'localhost:9091,localhost:9092,localhost:9093', 'group.id': 'mygroup', 'auto.offset.reset': 'earliest'}

    # Create the Kafka consumer instance
    consumer = Consumer(conf)

    # Subscribe to the Kafka topic and partition
    topic = 'segment-topic'
    consumer.assign([TopicPartition(topic, partition)])

    # Read messages from the Kafka topic and merge the segments
    batch_segments = {}
    merged = []
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'End of partition {partition} reached.')
            else:
                print(f'Error while consuming message from partition {partition}: {msg.error()}')
            continue

        # Deserialize the message from JSON
        message = json.loads(msg.value())
        #print(message)
        batch_id = message['batch_id']
        segment_id = message['segment_id']
        segment_data = message['data']

        # Add the segment to the batch_segments dictionary
        if batch_id not in batch_segments:
            batch_segments[batch_id] = {}
        batch_segments[batch_id][segment_id] = {"data": segment_data}

       
        
        # Merge all segments together
        segments = [segment["data"] for segments in batch_segments.values() for segment in segments.values()]
        
        heap = [(seg[0], i, seg) for i, seg in enumerate(segments) if seg]
        heapq.heapify(heap)
       
        while heap:
            val, index, seg = heapq.heappop(heap)
            merged.append(val)
            if len(seg) > 1:
                heapq.heappush(heap, (seg[1], index, seg[1:]))
            
        # Print the sorted elements in a single line
        print(",".join(map(str, merged)))

        # Clear the batch_segments dictionary for the next batch
        batch_segments = {}
        merged = []
# Create a thread pool and submit a task for each partition
with ThreadPoolExecutor(max_workers=10) as executor:
    for partition in range(10):
        executor.submit(consume_segment_topic, partition)
