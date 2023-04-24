from confluent_kafka import Consumer, KafkaError
import json
import heapq
import time

# Wait for 10 secs before subscribing
time.sleep(10)
# Create the Kafka consumer configuration
conf = {'bootstrap.servers': 'localhost:9091,localhost:9092,localhost:9093', 'group.id': 'mygroup', 'auto.offset.reset': 'earliest'}

# Create the Kafka consumer instance
consumer = Consumer(conf)


# Subscribe to the Kafka topic using the on_assign callback
consumer.subscribe(['segment-topic'])

# Read messages from the Kafka topic and merge the segments
segments = []
while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached.')
        else:
            print('Error while consuming message: {}'.format(msg.error()))
        continue

    # Deserialize the message from JSON
    message = json.loads(msg.value())
    segment_id = message['segment_id']
    segment = message['data']

    # Add the segment to the list of segments
    segments.append(segment)

    # Check if all segments have been consumed
    if len(segments) == 10:
        # Merge the segments using a priority queue
        merged = []
        heap = [(seg[0], i, seg) for i, seg in enumerate(segments) if seg]
        heapq.heapify(heap)
        while heap:
            val, index, seg = heapq.heappop(heap)
            merged.append(val)
            if len(seg) > 1:
                heapq.heappush(heap, (seg[1], index, seg[1:]))
        
        # Print the sorted elements in a single line
        print(*merged, sep=', ')
        with open('output/output.txt', 'a') as f:
            print(*merged, sep=', ', file=f)
        
        # Reset the list of segments
        segments = []
        
        print('Segments consumed successfully.')
