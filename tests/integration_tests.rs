use bob_ka::{BobConfig, TopicConfig, WebServerConfig};
use hyper::StatusCode;
use reqwest::Client;
use serde::{Deserialize, Serialize};

// multiple topics, msgs remain isolated
#[tokio::test]
async fn test_multiproducer_multiconsumer_allmessagesconsumed() {
    // start web server on background task
    tokio::task::spawn(async {
        bob_ka::start_web_server(BobConfig {
            temp_consumer_state: true,
            web_config: WebServerConfig { port: 1234 },
            topics: vec![TopicConfig {
                name: "test-topic".to_string(),
                compression: true,
                cap: 0,
                cap_tolerance: 0,
                temporary: true,
            }],
        })
        .await
        .unwrap();
    });

    println!("here");
    // producer a
    let a_handle = tokio::spawn(async {
        let client = Client::new();
        let producer_name = "a";
        for i in 0..20 {
            // println!("producing {i} for {producer_name}");
            let produce_resp = client
                .post("http://localhost:1234/produce/test-topic")
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
                .post("http://localhost:1234/produce/test-topic")
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
                .get("http://localhost:1234/consume/test-topic/abc/1")
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
                .post(format!("http://localhost:1234/ack/test-topic/abc/{msg_id}"))
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
            .get("http://localhost:1234/consume/test-topic/123/1")
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
            .post(format!("http://localhost:1234/ack/test-topic/123/{msg_id}"))
            .send()
            .await
            .unwrap();

        assert_eq!(ack_resp.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn test_multiproducer_topic_cap_observed() {
    tokio::task::spawn(async {
        bob_ka::start_web_server(BobConfig {
            temp_consumer_state: true,
            web_config: WebServerConfig { port: 1234 },
            topics: vec![TopicConfig {
                name: "test-topic".to_string(),
                compression: true,
                cap: 10,
                cap_tolerance: 5,
                temporary: true,
            }],
        })
        .await
        .unwrap();
    });

    println!("here");
    // producer a
    let a_handle = tokio::spawn(async {
        let client = Client::new();
        let producer_name = "a";
        for i in 0..50 {
            println!("producing {i} for {producer_name}");
            let produce_resp = client
                .post("http://localhost:1234/produce/test-topic")
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
                .post("http://localhost:1234/produce/test-topic")
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
