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
- see test_quick_start() in integration_tests.rs for an example:
```rust
#[tokio::test]
async fn test_quick_start() {
    // hello, welcome to bob-ka
    // this thing is a kafka inspired, lightweight, http/rest event thing
    // messages are organized into topics
    // each topic is N producer, N consumer
    // topic length caps are configurable, are count based, and trim on write
    // to deal with potentially tight consume loops, there is a server side sleep enforced when no
    // new messages are available (backoff_dialation_ms)
    // sled as the persistence layer, b+ tree/lsm tree read write perf in embedded db
    // example of producer:

    tokio::task::spawn(async {
        // producer
        // post request to /produce/{topic_name}
        // "event" data is in the request body as json
        // only single "event" generated per request
        for _ in 0..5 {
            let produce_resp = Client::new()
                .post("http://localhost:8011/produce/test-topic-quickstart")
                .json(&json!("data: event has occured!"))
                .send()
                .await
                .unwrap();

            // expect 200 OK if message persisted to topic successfully
            assert_eq!(produce_resp.status(), StatusCode::OK);
        }
    });

    // multiple producers can produce to same topic

    let client = Client::new();
    // loop { would be a loop in production situation
    for _ in 0..5 {
        // consumer
        // get request to /consume/{topic_name}/{consumer_id}/{batch_size}
        // consumer_id should be something unique, like a guid
        let consume_resp = client
            .get("http://localhost:8011/consume/test-topic-quickstart/abc/2")
            .send()
            .await
            .unwrap();

        if consume_resp.status() == StatusCode::NO_CONTENT {
            // no new messages for the consumer id = abc for topic "test-topic"
            // server enforced a "backoff_dialation_ms" sleep to avoid
            // tight looping consumers
        } else if consume_resp.status() == StatusCode::OK {
            // inspect body for "events"
            // expect body of this schema:
            // { events: [{id: 1, data: {prop1: "value1"}}, {id: 2, data: {prop1: "value2"}]}
            // where you get an array of events, each event having two keys: id and data
            // id is a sequence number assigned by the server
            // data is the data you posted in your produce request
            let msgs = consume_resp.json::<Events<Message>>().await.unwrap();
            let msg_id = msgs.events[0].id;
            let _data = &msgs.events[0].data;
            // process your data, and once successful, send an ACK to the server for the ID

            // ack messgae
            // post request to ack/{topic_name}/{consumer_id}/{msg_id}
            // server keeps track of where you are in the topic based on consumerid and last ack'd
            // message id
            let ack_resp = client
                .post(format!(
                    "http://localhost:8011/ack/test-topic-quickstart/abc/{msg_id}"
                ))
                .send()
                .await
                .unwrap();

            assert_eq!(ack_resp.status(), StatusCode::OK);
        }
    }
    // run this consumer in a loop! poll, process, and ack messages at your own leisure, without
    // worrying about abusive polling
    //
    // this solution is lightweight, kafka consumer/producer pattern inspried, with language
    // agnostic http rest apis, allowing you to deliver messages reliably in any environment
}
```

## Next Steps
- nodes of multiple bob-ka's managed by a "mothership" service
- - allows topics to be load balanced around multiple services/machines
- - service discovery
- - - nodes are given mothership address at config time
- - - if nodes have mothership, send message letting current location be known
- - - mothership keeps state of nodes, which topics they own, and where they are running
 
