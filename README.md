# prio

This repository implements a distributed fault tolerant priority queue.
The basic idea is you can spawn multiple workers and the topics (aka queue) are load balanced between these workers.
Every topic is assigned to an individual worker and every worker work on a fixed set of mutually exclusive topics.

It requires a zookeper instance up and running for its leader election and cong management needs.

Prio works with pluggable storage engine. We can write engine implementations baed on amy popular backends. It currently ships with 

- mysql(https://github.com/hextechpal/prio/tree/master/engine/mysql)
- memory(https://github.com/hextechpal/prio/tree/master/engine/memory) : this is only for tests purposes

## API

- RegisterTopic : Create a new topic 
- Enqueue: Add a new job to a particular topic
- Deque: Pops a job fron the topic based on queue
- Ack: Mark the job as completed

- Requeue: This is an internal api and not exposed. If the dequed task is not acked within 10 sec then the task is moved back to the queue and is eligible for redelivery


## Repo structure 

This repository is a set of go modules

- core(https://github.com/hextechpal/prio/tree/master/core) : This module consist of the basic interfaces and leader election code based on zookeeper. 
- engines/* : This directory code contain engine implementation modules. As descibed above mysql is implemented
- app : This implement an actual app on top of the prio modules and mysql engine
