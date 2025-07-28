# Project Zinger
- lightweight RESTful Kafka-esque event stream over HTTP, written in rust
- boblog or bob-ka or bob-queue

<img width="641" height="491" alt="image" src="https://github.com/user-attachments/assets/612410ec-70d3-4120-bb24-669dc454efd7" />

## Apis
- GET /health
- GET /consume/{topic_name}/{consumer_id}/{batch_size} -> returns JSON {"messages": [{"id": 123, "data": {...}}]
- POST /ack/{topic_name}/{consumer_id}/{last_msg_id}
- POST /produce/{topic_name} -> JSON in body, persisted as is
- GET /stats/{topic_name} -> returns JSON {"topic_name": "test", "topic_length": 123}
 
## Features
- N topic, N consumer, N producer
- consumer progress tracked per topic server side by way of ACK API
- topic length caps, trim on "produce"
- tight loop polling protection (backoff_dilation_ms)

## Quick Start
- see test_quick_start() in integration_tests.rs for an example
