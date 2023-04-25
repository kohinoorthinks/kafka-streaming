# Assignment Solution by Kohinoor Biswas

Thank you for inviting me to the coding assignment. I'm excited to be participating in this process.



## Approach

The basic project structure is as follows:

```bash
root/
 |-- output/
 |   |-- output.txt
 |-- data-stream-producer-partition.py
 |-- data-stream-scalable-concurrent-consumer.py
 |-- data-stream-scalable-consumer.py
 |-- data-stream-simple-consumer.py
 |-- docker-compose.yml
 |-- requirements.txt
 

```
## Task 1 - Reproducible Environment

Assumes docker and docker compose are installed and working
Assumes pip is available and python3 is on the path of the system.

### Instructions to Run Application

clone repo into home(~) of your user  
```bash
git clone https://github.com/kohinoorthinks/kafka-streaming.git
```
cd into the project root
```bash
cd kafka-streaming
```
Install requirements.txt

```bash
pip install -r requirements.txt

```
Start Kafka cluster
```bash
docker compose up -d
```
Start Producer 
```bash
python3 data-stream-producer-partition.py
```
Opena a new terminal and Start Simple Consumer 
```bash
cd kafka-streaming
python3 data-stream-simple-consumer.py
```
Both Producer and Consumer can be interrupted using CTRL + C interrupt

Interrupt simple consumer and start scalable consumer
 
```bash
cd kafka-streaming
python3 data-stream-scalable-consumer.py
```

Interrupt scalable consumer and start scalable concurrent consumer
 
```bash
cd kafka-streaming
python3 data-stream-scalable-concurrent-consumer.py
```
All consumer outputs are printed to console. 
Simple consumer produces output both on console and output/output.txt. Producer also prints input to output/output.txt so that input and output can be comapred.

## Task 2(a,b) - Simple Consumer

data-stream-simple-consumer.py meets the requirement by reading the messages from the Kafka topic in segments of 10 and merging them in a way that ensures the final output is sorted in ascending order. The code uses a priority queue to merge the segments, which guarantees that only a small portion of the data needs to be loaded into memory at any given time, thus avoiding the memory limitations mentioned in the requirement.

Furthermore, data-stream-simple-consumer.py writes the sorted output to a file rather than trying to keep everything in memory, which also satisfies the requirement of not loading all messages into memory. Overall, this code provides a scalable and memory-efficient solution for merging and sorting the segments of data.
data-stream-simple-consumer.py uses a priority queue to merge multiple segments that have been read from a Kafka topic. The segments are initially stored in a list called segments. When the number of segments in the list reaches 10, the code merges them using a priority queue and outputs the sorted elements in a single line.

The priority queue is implemented using the Python heapq module, which provides functions for working with heaps. In this code, the priority queue is created by creating a heap of tuples, where each tuple contains three elements: the first element is the value of the segment at the current position, the second element is the index of the segment in the original list, and the third element is the segment itself.

The heap is created using the heapify function, which takes an iterable and converts it into a heap. The heap is then used to merge the segments by repeatedly extracting the minimum element from the heap using the heappop function and adding it to the merged list. If the segment from which the minimum element was extracted has more elements, a new tuple is added to the heap with the next element from the segment. The process continues until all elements from all segments have been added to the merged list.

By using a priority queue, the code ensures that the elements are merged in a sorted order, without having to explicitly sort the entire list. This results in a more efficient merging process, especially for large lists.

## Task 3(a,b) - Scalable Consumer
### data-stream-scalable-consumer.py

data-stream-scalable-consumer.py reads messages from a Kafka topic using 10 different consumers, where each consumer is assigned to one partition of the topic. It then collects segments of a batch, where each batch has 10 segments, and stores them in a dictionary batch_segments. When all segments of a batch are collected, the segments are merged using a priority queue and the merged segments are printed.

The priority queue is used to sort the elements in all segments. The code creates a heap of tuples, where each tuple has three elements: the first element is the first element of a segment, the second element is the index of the consumer that fetched the segment, and the third element is the entire segment. The heap is initialized with these tuples, and it is then transformed into a priority queue using heapq.heapify(heap). The while loop then pops the smallest element from the heap, appends it to the list merged, and pushes the rest of the segment back onto the heap using heapq.heappush(heap, (seg[1], index, seg[1:])) if the segment is not empty. This continues until the heap is empty, and merged contains all elements in the merged segments in sorted order.

### data-stream-scalable-consumer.py

The data-stream-scalable-consumer.py is using a thread pool with Kafka consumers to consume and merge messages from different partitions of a Kafka topic.

The consume_segment_topic function is defined to consume messages from a single partition of the Kafka topic. It creates a Kafka consumer instance, subscribes to the topic and partition, and starts polling messages from the partition. When a message is received, it deserializes the message from JSON, extracts the batch ID, segment ID, and segment data, and adds the segment to the batch_segments dictionary.

Once all segments for a batch are received, the function merges all segments together using a heap. The heap stores the first element of each segment as a tuple with its index and the segment itself. The heap is sorted based on the first element of each segment tuple, and the smallest element is popped from the heap and added to the merged list. If the segment has more than one element, the heap is updated with the next element from the segment.

After all segments for a batch are merged, the function prints the sorted elements in a single line and clears the batch_segments dictionary for the next batch.

The main part of the code creates a thread pool with 10 workers and submits a task for each partition to the consume_segment_topic function using the executor.submit() method. This allows the function to be run concurrently on different partitions, improving the overall performance of the message processing.
