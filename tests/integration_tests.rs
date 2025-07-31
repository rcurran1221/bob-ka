use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use ::futures::future::join_all;
use hyper::StatusCode;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::time::{Instant, sleep};

#[ignore]
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
    // example client side app:

    let client = Client::new(); // use a single http client for pooling

    // producer
    // post request to /produce/{topic_name}
    // "event" data is in the request body as json
    // only single "event" generated per request
    let produce_resp = client
        .post("http://localhost:8011/produce/test-topic-quickstart")
        .json(&json!("data: event has occured!"))
        .send()
        .await
        .unwrap();

    // expect 200 OK if message persisted to topic successfully
    assert_eq!(produce_resp.status(), StatusCode::OK);

    // multiple producers can produce to same topic

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
        // { events: [{id: 1, data: {prop1: "value1"}}, ..]}
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
    // run this consumer in a loop! poll, process, and ack messages at your own leisure, without
    // worrying about abusive polling
    //
    // this solution is lightweight, kafka consumer/producer pattern inspried, with language
    // agnostic http rest apis, allowing you to deliver messages reliably in any environment
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_stress_test() {
    let n_workers = 100;
    let mut futures = vec![];
    let timings = Arc::new(Mutex::new(Vec::<Duration>::new()));
    for n in 0..n_workers {
        let handle = tokio::spawn({
            let timings = timings.clone();
            async move {
                let client = Client::new();
                for i in 0..100 {
                    let now = Instant::now();
                    let produce_resp = client
                        .post("http://localhost:8011/produce/test-topic-stress")
                        .json(&Message {
                            event_name: format!("{n}{i}").to_string(),
                            event_data: "this is data".to_string(),
                            event_num: i,
                        })
                        .send()
                        .await
                        .unwrap();

                    assert_eq!(produce_resp.status(), StatusCode::OK);
                    let duration = now.elapsed();
                    timings.lock().unwrap().push(duration);
                }
            }
        });
        futures.push(handle);
    }

    join_all(futures).await;

    let stats_resp = Client::new()
        .get("http://localhost:8011/stats/test-topic-stress")
        .send()
        .await
        .unwrap();

    match stats_resp.json::<StatsResponse>().await {
        Ok(resp) => {
            assert_eq!(resp.topic_name, "test-topic-stress");
            assert!(resp.topic_length >= 100);
        }
        Err(e) => panic!("error translating the json: {e}"),
    };

    // for res in timings.lock().unwrap().iter() {
    //     println!{"{:?}", res};
    // }
}

#[derive(Deserialize, Debug)]
struct StatsResponse {
    topic_name: String,
    topic_length: usize,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multiproducer_multiconsumer_allmessagesconsumed() {
    // producer a
    let a_handle = tokio::spawn(async {
        let client = Client::new();
        let producer_name = "a";
        for i in 0..20 {
            // println!("producing {i} for {producer_name}");
            let produce_resp = client
                .post("http://localhost:8011/produce/test-topic")
                .json(&Message {
                    event_name: format!("{producer_name}{i}").to_string(),
                    event_data: "this is data".to_string(),
                    event_num: i,
                })
                .send()
                .await
                .unwrap();

            assert_eq!(produce_resp.status(), StatusCode::OK);
        }
    });

    // producer b
    let b_handle = tokio::spawn(async {
        let client = Client::new();
        let producer_name = "b";
        for i in 0..20 {
            // println!("producing {i} for {producer_name}");
            let produce_resp = client
                .post("http://localhost:8011/produce/test-topic")
                .json(&Message {
                    event_name: format!("{producer_name}{i}").to_string(),
                    event_data: "this is data".to_string(),
                    event_num: i,
                })
                .send()
                .await
                .unwrap();

            assert_eq!(produce_resp.status(), StatusCode::OK);
        }
    });

    let (_, _) = tokio::join!(a_handle, b_handle);

    tokio::spawn(async {
        let mut last_a_msg = 0;
        let mut last_b_msg = 0;
        for i in 0..50 {
            println!("consuming {i} ");
            let client = Client::new();
            let consume_resp = client
                .get("http://localhost:8011/consume/test-topic/abc/1")
                .send()
                .await
                .unwrap();

            if consume_resp.status() == StatusCode::NO_CONTENT {
                if last_b_msg < 20 || last_a_msg < 20 {
                    panic!(
                        "got no content but additional messages exist: b state: {last_b_msg}, a state: {last_a_msg}",
                    );
                };
                continue;
            }
            let resp_body = consume_resp.text().await.unwrap();
            println!("{resp_body}");
            let msgs: Events<Message> = serde_json::from_str(&resp_body).unwrap();
            assert_eq!(msgs.events.len(), 1);
            if msgs.events[0].data.event_name.contains("a") {
                assert!(msgs.events[0].data.event_num >= last_a_msg);
                last_a_msg += 1;
            } else {
                assert!(msgs.events[0].data.event_num >= last_b_msg);
                last_b_msg += 1;
            }
            let msg_id = msgs.events[0].id;

            // ack messgae
            let ack_resp = client
                .post(format!("http://localhost:8011/ack/test-topic/abc/{msg_id}"))
                .send()
                .await
                .unwrap();

            assert_eq!(ack_resp.status(), StatusCode::OK);
        }
    });

    let mut last_a_msg = 0;
    let mut last_b_msg = 0;
    for _ in 0..50 {
        let client = Client::new();
        let consume_resp = client
            .get("http://localhost:8011/consume/test-topic/123/1")
            .send()
            .await
            .unwrap();

        if consume_resp.status() == StatusCode::NO_CONTENT {
            if last_b_msg < 20 || last_a_msg < 20 {
                panic!(
                    "got no content but additional messages exist: b state: {last_b_msg}, a state: {last_a_msg}"
                );
            };
            continue;
        }
        let resp_body = consume_resp.text().await.unwrap();
        println!("{resp_body}");
        let msgs: Events<Message> = serde_json::from_str(&resp_body).unwrap();
        assert_eq!(msgs.events.len(), 1);
        if msgs.events[0].data.event_name.contains("a") {
            assert!(msgs.events[0].data.event_num >= last_a_msg);
            last_a_msg += 1;
        } else {
            assert!(msgs.events[0].data.event_num >= last_b_msg);
            last_b_msg += 1;
        }
        let msg_id = msgs.events[0].id;

        // ack messgae
        let ack_resp = client
            .post(format!("http://localhost:8011/ack/test-topic/123/{msg_id}"))
            .send()
            .await
            .unwrap();

        assert_eq!(ack_resp.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn test_multiproducer_topic_cap_observed() {
    // producer a
    let a_handle = tokio::spawn(async {
        let client = Client::new();
        let producer_name = "a";
        for i in 0..50 {
            println!("producing {i} for {producer_name}");
            let produce_resp = client
                .post("http://localhost:8011/produce/test-topic-cap")
                .json(&Message {
                    event_name: format!("{producer_name}{i}").to_string(),
                    event_data: "this is data".to_string(),
                    event_num: i,
                })
                .send()
                .await
                .unwrap();

            assert_eq!(produce_resp.status(), StatusCode::OK);
        }
    });

    // producer b
    let b_handle = tokio::spawn(async {
        let client = Client::new();
        let producer_name = "b";
        for i in 0..50 {
            println!("producing {i} for {producer_name}");
            let produce_resp = client
                .post("http://localhost:8011/produce/test-topic-cap")
                .json(&Message {
                    event_name: format!("{producer_name}{i}").to_string(),
                    event_data: "this is data".to_string(),
                    event_num: i,
                })
                .send()
                .await
                .unwrap();

            assert_eq!(produce_resp.status(), StatusCode::OK);
        }
    });

    let (_, _) = tokio::join!(a_handle, b_handle);
}

#[tokio::test]
async fn test_consumer_batch_size() {
    // produce some messages
    // consume with batch size N
    // enumerate and ack each successfully
}

#[tokio::test]
async fn test_backpressure_no_content() {
    let now = Instant::now();
    let client = Client::new();
    let consume_resp = client
        .get("http://localhost:8011/consume/test-topic-backpressure/abc/1")
        .send()
        .await
        .unwrap();

    let dur = Instant::now().duration_since(now);
    assert_eq!(consume_resp.status(), StatusCode::NO_CONTENT);
    print!("{}", dur.as_millis());
    assert!(dur > Duration::from_millis(1000));
}

#[ignore]
#[tokio::test]
async fn test_time_based_retention() {
    let client = Client::new();
    let producer_name = "a";
    for i in 0..24 {
        println!("producing {i} for {producer_name}");
        let produce_resp = client
            .post("http://localhost:8011/produce/test-topic-time-retention")
            .json(&Message {
                event_name: format!("{producer_name}{i}").to_string(),
                event_data: "this is data".to_string(),
                event_num: i,
            })
            .send()
            .await
            .unwrap();

        assert_eq!(produce_resp.status(), StatusCode::OK);

        // sleep for 5 seconds
        sleep(Duration::from_secs(5)).await;
    }

    let stats_resp = Client::new()
        .get("http://localhost:8011/stats/test-topic-time-retention")
        .send()
        .await
        .unwrap();

    let len_topic = match stats_resp.json::<StatsResponse>().await {
        Ok(resp) => {
            println!("{:?}", resp);
            assert_eq!(resp.topic_name, "test-topic-time-retention");
            assert!(resp.topic_length >= 11 && resp.topic_length <= 13);
            resp.topic_length
        }
        Err(e) => panic!("error translating the json: {e}"),
    };

    let consume_resp = client
        .get("http://localhost:8011/consume/test-topic-time-retention/123/30")
        .send()
        .await
        .unwrap()
        .json::<Events<Message>>()
        .await;

    // should at least have event_nums from 23 to 10ish?, should not have 0 through 9
    let mut event_nums: Vec<u16> = vec![];
    for event in consume_resp.unwrap().events {
        event_nums.push(event.data.event_num);
    }

    assert_eq!(len_topic, event_nums.len());
    assert!(event_nums.contains(&23));
    assert!(event_nums.contains(&13));
    assert!(!event_nums.contains(&9));
    assert!(!event_nums.contains(&0));
}

#[derive(Serialize, Deserialize)]
struct Message {
    event_data: String,
    event_name: String,
    event_num: u16,
}

#[derive(Serialize, Deserialize)]
struct Event<T> {
    id: u64,
    data: T,
}

#[derive(Serialize, Deserialize)]
struct Events<T> {
    events: Vec<Event<T>>,
}
