# Project Zinger
- lightweight RESTful Kafka-esque event stream over HTTP, written in rust
- boblog or bob-ka or bob-queue


## Project Goals

### v1
- HTTP endpoint that enables a caller to poll for real-time data updates
  - Use Kafka consumer pattern as a "pattern" for the APIs
  - Consume, ack, subscribe(?)
  - Server keeps track of consumer state
    - Multi-consumer "group" support, single "consumer" per group for now
  - Messages live in memory; when the server process ends, the message queue is cleared
    - Example: If compliance is restarted, there is no obligation to rebuild the message queue
  - Messages are "produced" from an existing process, e.g., notification or event from another part of the system
    - Assume the process has a message in event form in memory, and it is added to the queue by an independent thread
    - Allow for a test "producer" that produces X events every N seconds
  - Retention of the in-memory queue is configurable per topic, with time-based trimming on write (similar to the event stream pattern)

### Future
- Back queue with disk persistence
  - If the process serving messages fails, it will rebuild the queue from disk
- Process exposes a "producer" API, modeled after Kafka producer
- OAuth token-based authorization
- Producer and consumer role-based security per topic

<img width="641" height="491" alt="image" src="https://github.com/user-attachments/assets/612410ec-70d3-4120-bb24-669dc454efd7" />
