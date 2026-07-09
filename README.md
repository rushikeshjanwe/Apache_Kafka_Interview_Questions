# Apache_Kafka_Interview_Questions

What is Kafka 
Kafka is an distributed Event streaming platform used for asynchronous communication between services.

Stores data like a log → can read again

How does Kafka work internally?
Producer sends messages to a topic → Topic is divided into partitions → Messages are stored on brokers → Consumers read messages using offsets.


Why Kafka over Rest 
REST APIs are synchronous and tightly coupled—you have to call each API and wait for its response. Kafka is asynchronous and loosely coupled—you send one message, and multiple services process it independently at the same time.

1. Producer :- Sends data (messages) to Kafka topics.                                                                                                      
2. Topic :- Acts as a mediator and stores data between producers and consumers. 
3. Consumer :- Reads data from topics. 
4. Partitions :- Split topics into smaller parts for faster and parallel processing.   Or We can add multiple partitions for faster and parallel processing.
5. Brokers ( Multiple brokers ie Kafka cluster ) :- Multiple servers that share the load so one system is not overloaded.
6. Offset :- Keeps track of which data is already read, so consumers can restart from the same point. 
7. Consumer Group :- We can add multiple consumers to share the data and process it faster.

A Kafka consumer group provides horizontal scalability and load balancing by distributing topic partitions across consumers, ensuring that each partition is consumed by only one consumer within the group.
• If you have more consumers than partitions, some consumers sit idle.

Q1. What are the common reasons for a Kafka consumer crash?
 High load, Out of Memory (OOM), application bugs, unhandled exceptions, database/network failures, or infrastructure issues.

Q2. How does Kafka recover when a consumer crashes?
 Kafka detects the missing heartbeats, triggers a rebalance, assigns the partitions to another consumer, and resumes from the last committed offset.

Q3. Who is responsible for fixing a consumer crash?
Developers fix application issues, while the DevOps/Infrastructure team handles server, Kubernetes, or network-related issues.

Q. What are Topics and Partitions in Kafka?
A Topic is a communication medium between producers and consumers where producers publish 
messages and consumers read them. 
A Topic is divided into multiple partitions, which enable parallel processing and improve scalability.
• A topic with 6 partitions can be consumed by up to 6 consumers in parallel (within a group)
• Messages with the same key always go to the same partition (ordering guarantee per key)
• Partitions are distributed across brokers for load balancing


Q. What is Replication?
Replication is the process of creating multiple copies of a partition to ensure fault tolerance and high availability.

Q. What is Replication Factor?
Replication Factor is the total number of copies of a partition maintained across different brokers, including one Leader and the remaining Followers.

• Replication = The process of keeping these copies synchronized. 
• Replication Factor = 3 = The total number of copies. 

Q. What is ISR (In-Sync Replica)?
ISR is the set of follower replicas that are fully in sync with the leader and are eligible to become the new leader if the current leader fails.

Q. Why does Kafka have partitions?
Partitions enable horizontal scalability and parallel processing. 
They allow Kafka to distribute load across brokers and let multiple consumers in the same consumer group process data concurrently.

Q. If partitions increase performance…?
Why not create 10,000 partitions?
Every partition has overhead:
	• Memory 
	• Replication 
More partitions ≠ infinitely better performance.
There's a tradeoff.

Q. How does Kafka Scale 
Kafka scales by adding more partitions and consumers, allowing parallel processing of data.

Kafka scales horizontally by adding more brokers, increasing partitions for parallelism, 
and running more producer and consumer instances based on the application's 
throughput requirements.

Q. What Happens if a Kafka Broker Goes Down?
If a Kafka broker goes down, the leader partition on that broker is replaced by an ISR (In-Sync Replica), 
which becomes the new leader. 
Producers and consumers automatically start communicating with the new leader, so the system continues to work with minimal disruption.

Q. What is Producer Acknowledgement (acks)?
acks controls how many broker acknowledgements the producer requires before considering a write successful.

Q. What Happens When a New Consumer Joins a Consumer Group?
When a new consumer joins a consumer group, Kafka performs a rebalance, redistributing partitions among all consumers so the workload is shared evenly. During the rebalance, message consumption pauses briefly.

Q. What happens if consumer crashes?
Kafka uses offsets to track progress, so the consumer resumes from the last committed offset.

Q. Consumer Lag? 
Consumer are slower than the producer.
Consumer lag is the difference between the latest offset and the consumer's committed offset.

Q. How do you handle duplicates?
We use idempotency, ensuring the same message is not processed multiple times.

Q. How do you ensure no data loss?
By using replication, acknowledgements (acks=all), and committing offsets after processing.

Q. Does Kafka guarantee order?
Kafka guarantees ordering only within a partition. To maintain order, we use a key so related 
messages go to the same partition.

Q. What happens when a new consumer joins?
Kafka rebalances partitions among consumers, which can temporarily pause processing.

Q. What if consumer is slow?
Consumer lag means consumers are slower than producers.
Consumer lag increases, and we handle it by scaling consumers or optimizing processing.

What was YOUR role?
I worked on producer and consumer logic, handled retries, idempotency, error handling, and ensured reliable message processing. Kafka infrastructure was managed by DevOps.

What is replication?
Kafka stores data in multiple brokers to ensure fault tolerance and prevent data loss.

Q. What is a dead letter queue?
A separate topic where failed messages are stored for later analysis or reprocessing.
🎯 Why needed?
	• Don’t block system 
	• Store failed data 
	• Debug later

How does Kafka work?
Producers send messages to topics, which are divided into partitions for scalability. Each message has an offset, and consumers in consumer groups read and process messages in parallel.

What challenges have you faced?
Common challenges include consumer lag, message duplication, and rebalancing. We handled them using scaling, idempotency, and proper configuration.

What was your role?
I worked on Kafka producers and consumers, handling message processing, retries, idempotency, and error handling. Kafka infrastructure was managed by the DevOps team.


1. Replication → Kafka replicates partitions across brokers so data isn't lost if a broker fails.
2. Sharding (Partitioning) → Messages are distributed across partitions for scalability and parallel processing.
3. Balancing → Consumers in the same consumer group share partitions and workload.
4. Circuit Breaker → If a service (e.g., Payment) calls an external bank API that's failing, it opens the circuit to prevent cascading failures.
5. Retry & Dead Letter Queue (DLQ) → Failed messages can be retried or moved to a DLQ for later analysis.
6. Idempotency → Prevents duplicate processing when Kafka redelivers a message.
7. Service Discovery → Services find each other dynamically instead of hardcoding addresses.
8. API Gateway → Handles client requests before they reach backend services.
9. Distributed Tracing → Tools like Zipkin or Jaeger help trace a request across multiple services.
10. Monitoring & Logging → Metrics (Prometheus), dashboards (Grafana), logs (ELK/Splunk) help monitor the entire flow.



🔹 Key (VERY IMPORTANT)
	Key decides partition and helps maintain ordering

🔹 Ordering
	Kafka guarantees ordering only within a partition

🔹 Scaling
	Kafka scales by increasing partitions and consumers

🔹 Consumer Crash
	Consumer resumes from last committed offset

🔹 Duplicate Handling
	Use idempotency to avoid duplicate processing

🔹 Consumer Lag
	Happens when consumers are slower than producers
	👉 Fix: scale consumers / optimize processing

🔹 Rebalancing
	Kafka redistributes partitions when consumers join/leave
	👉 Causes temporary pause

🔹 Dead Letter Queue (DLQ)
	Failed messages are stored in a separate topic for later analysis

🔹 Failure Handling
	• Retry 
	• DLQ 
	• Idempotency 

🔹 Your Role (IMPORTANT)
	I worked on producer and consumer logic, handled retries, idempotency, and error handling. Kafka infra was managed by DevOps.

🎯 FINAL INTERVIEW LINE
	Kafka is a distributed event streaming platform where producers send messages to topics, which are divided into partitions for scalability. Consumers in consumer groups process messages using offsets. We handle failures using retries, idempotency, and dead letter queues.

	1. How do you troubleshoot if yr consumer lag is increasing ? I would check consumer lag, consumer logs, and identify any slow processing, database, or external API bottlenecks.
	
	2. You're reading from kafka and writing to DB. 
	If db write fails after consuming but before offset commit, how do u prevent data loss or duplication.
	Commit the offset only after the database write succeeds and use idempotency to avoid duplicate processing.
	
	3. What mechanism is used to ensure no data lost when consumer down and producer keep sending events ? 
	Use replication, retries, manual offset commits, idempotency, and a DLQ to ensure reliable message processing.
	
	4. I have one question
	There was flood of messages from producers
	And we increased consumer to process these some how it's ok now but duplicate message processing started 
	How you solve this without doing any backend code change
	From <https://www.linkedin.com/posts/sahil-wankhade-858015185_kafka-javadevelopers-springboot-share-7385687145878818816-FCVl/> 
	I would tune Kafka configurations such as retries, offset commits, and partition assignments to reduce duplicate processing.
	
	
	Q2. What is a Broker?
	Answer:
	A Broker is a Kafka server that stores topics and partitions. Multiple brokers together form a Kafka cluster, allowing Kafka to scale horizontally and provide fault tolerance.
	
	Q3. What is a Topic?
	Answer:
	Acts as a mediator and stores data between producers and consumers. 
	A Topic is a logical category used to store similar events. 
	For example, all order-related events are stored in the orders topic, while payment-related events are stored in the payments topic.
	
	Q4. What is a Partition, and why do we need it?
	Answer:
	A Partition is a smaller division of a topic. Partitions enable parallel processing, higher throughput, and horizontal scalability because different partitions can be distributed across multiple brokers and consumed simultaneously.
	
	Q5. What is Replication Factor (RF)?
	Answer:
	Replication Factor specifies how many copies of each partition Kafka should maintain.
	Example:
		• RF = 3
		• One Leader replica
		• Two Follower replicas
	This provides high availability and fault tolerance.
	
	Q6. How do we decide the number of Brokers, Partitions, and Replication Factor?
	Answer:
	These are architectural decisions made by developers or architects based on business requirements.
		• Brokers: Based on expected traffic, storage requirements, scalability, and fault tolerance.
		• Partitions: Based on the required parallelism and throughput.
		• Replication Factor (RF): Based on the desired availability, reliability, and acceptable infrastructure cost.
	Kafka does not decide these values automatically.
	
	Q7. What are Leader and Follower replicas?
	Answer:
	Each partition has multiple replicas according to the Replication Factor.
		• Leader: Handles all producer writes and consumer reads (by default).
		• Followers: Continuously replicate data from the Leader and stay synchronized. They do not accept producer writes.
	If the Leader fails, one of the synchronized followers is promoted as the new Leader.
	
	Q8. Why can't producers write directly to followers?
	Answer:
	Kafka maintains a single source of truth. If producers wrote to multiple replicas independently, the data could become inconsistent due to different write orders. Therefore, all writes go to the Leader, and Followers only replicate the Leader's data.
	
	
	Q9. Can the Replication Factor be greater than the number of Brokers?
	Answer:
	No.
	Replication Factor must always be less than or equal to the number of brokers.
	Example:
		• Brokers = 2
		• RF = 3 ❌ Not possible
	Kafka cannot place three replicas on only two brokers.
	
	Q10. How many Leaders exist in a Kafka topic?
	Answer:
	There is exactly one Leader per partition.
	Example:
		• Partitions = 6
		• Replication Factor = 3
	Result:
		• Total Leaders = 6
		• Total Followers = 12
		• Total Replicas = 18
	Leaders are distributed across brokers to balance the load.
	
	
	Quick Formulas
		• Total Leaders = Number of Partitions
		• Total Replicas = Partitions × Replication Factor
		• Replication Factor ≤ Number of Brokers
		• One Partition = One Leader + (RF − 1) Followers
	
	
	
Q1. What is an Offset?
Answer:
An Offset is the unique sequential number assigned to every message within a partition.
Kafka uses offsets to uniquely identify each message inside a partition.
	
Q2. Why do we need Offsets?
Answer:
Offsets help Kafka track which messages have already been processed by a consumer.

Q3. Why doesn't Kafka delete a message after a consumer reads it?
Kafka doesn't delete messages after they are read because the same message may be needed again.
For example
:
Multiple consumers may read the same message.
Kafka deletes messages only when the configured retention time or size limit is reached, not when a consumer reads them.

Q4. What is a Consumer?
A Consumer is an application that reads messages from Kafka topics.

Q5. Why do we need Consumer Groups?
Consumer Groups provide:
	• Parallel processing 
	• Scalability 
	• Load balancing 
Instead of one consumer processing all partitions, Kafka distributes the partitions across multiple consumers.

Q6. What is a Consumer Group?
A Consumer Group is a collection of consumers working together to process messages from a topic.
Kafka distributes partitions among consumers within the same group for parallel processing.

Q7. Can two consumers read the same message?
It depends.
Same Consumer Group
❌ No.
Only one consumer processes a partition at a time.

Different Consumer Groups
✅ Yes.
Each consumer group maintains its own offsets, so every group can read the same messages independently.

Q8. What happens if there are more consumers than partitions?
Extra consumers remain idle because one partition can be assigned to only one consumer within the same consumer group.

Q9. What happens if there are more partitions than consumers?
Each consumer handles multiple partitions.

Q10. What happens if a consumer crashes?
Answer:
Kafka does not lose track of progress.
The consumer's last committed offset is stored.

Q11. What is Rebalancing?
Answer:
Rebalancing is the process of redistributing partitions among consumers in a consumer group.
It occurs when:
	• A new consumer joins. 
	• A consumer leaves or crashes. 
	• The number of partitions changes. 
Kafka reassigns partitions so the workload stays balanced.

Q12. What happens if a consumer is down for two days?
Answer:
Kafka retains the messages according to the topic's retention policy.
When the consumer comes back, it continues reading from its last committed offset (provided the messages are still retained).

Q13 What could be the reasons for increasing consumer lag? 
Consumer lag increases when consumers are unable to process messages as fast as producers are producing them. This can happen due to slow consumer processing, insufficient consumers or partitions, consumer crashes, rebalancing, or slow downstream systems like databases or external APIs.

Quick Revision Formulas
	• Offset = Position of a message within a partition. 
	• One Partition → One Consumer (within the same Consumer Group). 
	• Different Consumer Groups can read the same messages. 
	• Consumers > Partitions → Extra consumers remain idle. 
	• Consumers < Partitions → Consumers process multiple partitions. 
	• Offsets track consumer progress. 
	• Kafka deletes messages based on retention, not because they were consumed. 
	• Rebalancing redistributes partitions when consumers join or leave.



Q1. Consumers are running fine, but Kafka lag is continuously increasing. How will you identify the issue?
 I would compare producer and consumer throughput and check consumer logs for slow processing or downstream bottlenecks.   ///
I would first check the consumer lag, then verify if producers are sending messages faster than consumers can process them. Next, I would check consumer logs, and identify if slow processing or downstream services are causing the lag.

Q2. What could be the reasons for increasing consumer lag?
consumer lag occurs when consumers process messages slower than producers generate them. ///
Consumer lag increases when consumers process messages slower than producers generate them. Common reasons include slow business logic, database latency, insufficient consumers or partitions, consumer crashes, rebalancing, or infrastructure bottlenecks.

Q3. How will you reduce Kafka lag? Increase consumers (up to the number of partitions), increase partitions if needed, and optimize consumer processing. ///
Increase consumers (up to the number of partitions), increase partitions if needed, optimize consumer processing, batch database writes, reduce slow external calls, and scale consumer instances horizontally.

Q4. What is a Dead Letter Queue (DLQ)?

A DLQ stores messages that repeatedly fail processing after configured retries. This prevents one bad message from blocking the consumer while allowing failed messages to be analyzed or reprocessed later.

Q5. No transaction can be left unprocessed. How would you architect it?
Use Kafka with replication, manual offset commits, retries, DLQ for unrecoverable failures, idempotent processing, and transactions where required to ensure reliable processing.

Q6. How does Kafka avoid duplicate messages?
Kafka can reduce duplicates using an idempotent producer and transactions. On the consumer side, applications should implement idempotent processing because retries can still occur.

Q7. How is data not lost when the consumer is down?
Answer:
	Kafka stores messages in the topic based on the configured retention period. Producers continue writing, and when the consumer comes back, it resumes from its last committed offset.

Q8. How does the producer know the consumer has received the data?
It doesn't.
The producer only receives an acknowledgement (ack) from the Kafka broker, not from the consumer. Producers and consumers are completely decoupled.
 
Q. How Do You Retry Failed Kafka Messages?
Retry the message a few times, and if it still fails, send it to a Dead Letter Topic (DLT).

Q. What is Kafka Streams?
Kafka Streams is a Java library used to process and transform Kafka data in real time. It runs inside your application.

Q. Difference Between Kafka Streams and Spark Streaming?
	• Kafka Streams: Java library, no separate cluster. 
	• Spark Streaming: Distributed framework, requires a Spark cluster. 

Q. What is Auto Offset Reset?
Auto Offset Reset defines where a consumer starts reading messages if no previous offset exists.
	• earliest → Read from the beginning. 
	• latest → Read only new messages.
	
Q. What is Consumer Lag?
Consumer Lag is the difference between the latest offset (end of partition) and the consumer’s committed offset.It tells you how far behind a consumer is.
Q. What is Kafka Retention Policy?
Kafka retains messages based on your configuration — not based on consumption. 

🏭 Section 9: Spring Boot Integration

Q. What is KafkaTemplate in Spring Boot?

KafkaTemplate is a Spring Boot class used to send messages to Kafka topics.

Q. What is @KafkaListener?
@KafkaListener is a Spring Boot annotation used to consume (read) messages from a Kafka topic.

Q. How Do You Handle Kafka Consumer Exceptions in Spring Boot?
Handle consumer exceptions using error handlers, retries, and a Dead Letter Topic (DLT) for failed messages.

🚨 Section 10: Production Scenarios

Q. Common Production Issues in Kafka
Answer:
	• Consumer lag 
	• Broker failure 
	• Message duplication 
	• Message loss 
	• High latency 
	• Disk space issues 
	• Rebalancing delays 

Q. How Do You Monitor Kafka in Production?
Monitor consumer lag, broker health, ISR status, disk usage, and throughput 
using tools like Prometheus and Grafana.

Q. What Happens If Partition Count is Increased Later?
Increasing the partition count improves parallelism, but it can change how new messages are distributed across partitions. Existing messages remain in their original partitions.

Q. Can Kafka Guarantee Message Ordering Across Partitions?
No. Kafka only guarantees ordering within a single partition.

Q. How does Kafka achieve exactly-once semantics?
Through idempotent producers and transactional APIs ensuring no duplicate processing.

Q8. Explain Kafka’s offset management.
Offsets track the position of a consumer in a partition. Managed manually or 
automatically with enable.auto.commit.



===============================================================================

Kafka Producer
 
1. How does a producer decide which partition to send a message to?
If a key is provided → Kafka hashes the key and sends the message to the same partition. 
If no key is provided → Kafka distributes messages across partitions.

2. What happens after a producer sends a message?
The producer sends the record to the leader partition. After the leader acknowledges (and replicas sync depending on configuration), the producer receives an acknowledgment.

3. What is Producer Retry?
If sending fails due to a temporary issue, the producer retries automatically 

4. What happens if the partition count is increased later?
Kafka creates new partitions, but existing data remains in the old partitions. 
Messages with the same key may now hash to a different partition, 
so ordering is guaranteed only within each partition.

5. What happens if the leader broker crashes while producers are sending messages?
Kafka elects a new leader from the ISR. Producers automatically refresh metadata and continue sending messages to the new leader.
                                 
IMP
6. Can two consumers in the same consumer group read the same partition?
No. A partition is assigned to only one consumer within a group.

7. Can two different consumer groups read the same partition?
Yes. Each consumer group maintains its own offset.

8. Where are Offsets stored?
Kafka stores consumer offsets in the internal __consumer_offsets topic.

9.  What is Offset Commit?
Offset commit is the process of saving the last successfully processed message so that,
after a restart or crash, the consumer can resume from that point.

10. What happens if a consumer crashes before committing the offset?
Another consumer in the same consumer group takes over the partition during rebalancing 
and starts reading from the last committed offset

11. What happens if a consumer is down for 2 days? ( Default retention period is 7 days )
If Kafka's retention period has not expired, the consumer resumes from its last committed offset. 
If the messages have already been deleted due to retention, they cannot be recovered.

12. What is Rebalancing?
Rebalancing occurs whenever consumers join or leave a consumer group. Kafka redistributes partitions among the active consumers.


13. When does Rebalancing happen?
	• A new consumer joins the group.
	• A consumer crashes or leaves.
	• The number of partitions changes.
	• A consumer stops sending heartbeats.

14.  What are Kafka Delivery Guarantees? 
By Default : At-least once
Kafka supports three delivery guarantees:
	• At-most-once : Message is delivered 0 or 1 time. No duplicates, but message loss is possible.
	• At-least-once : Message is delivered 1 or more times. No message loss, but duplicates are possible.
	• Exactly-once : Message is delivered exactly 1 time. No duplicates and no message loss.
	
15.  Why does duplicate processing happen?
If a consumer processes a message but crashes before committing the offset, another consumer reads the same message again from the last committed offset.

16. How does Kafka work internally?
Producer sends messages to a topic → Topic is divided into partitions → Messages are stored on brokers → Consumers read messages using offsets.

17. What is ZooKeeper?
ZooKeeper was used to manage Kafka metadata, broker registration, and controller election.

18.  What is KRaft?
KRaft is Kafka's built-in metadata management system that replaces ZooKeeper.

19. Why did Kafka remove ZooKeeper?
To simplify cluster management, improve scalability, and reduce operational complexity.
 
20. What is Consumer Lag?
Consumer lag is the difference between the latest offset and the consumer's committed offset.

21. How do you improve Kafka Producer and Consumer performance?
To improve Kafka performance, we can increase the number of partitions for parallel processing,
add more consumers to process messages faster, and scale producers if higher message throughput is required.
![Uploading image.png…]()
