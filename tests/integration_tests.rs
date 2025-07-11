use bob_ka::{BobConfig, TopicConfig, WebServerConfig};
use hyper::StatusCode;
use reqwest::Client;
use serde::{Deserialize, Serialize};

//test tokio
// batch size greater than 1
// multiple topics, msgs remain isolated
// retention once implemented
#[tokio::test]
async fn test() {
    // start web server on background task
    tokio::task::spawn(async {
        bob_ka::start_web_server(BobConfig {
            web_config: WebServerConfig { port: 1234 },
            topics: vec![TopicConfig {
                name: "test-topic".to_string(),
                compression: true,
                cap: 0,
                temporary: true,
            }],
        })
        .await
        .unwrap();
    });

    // producer a
    tokio::spawn(async {
        let client = Client::new();
        let producer_name = "a";
        for i in 0..5 {
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
    tokio::spawn(async {
        let client = Client::new();
        let producer_name = "b";
        for i in 0..5 {
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

    tokio::spawn(async {
        let mut last_a_msg = 0;
        let mut last_b_msg = 0;
        for _ in 0..15 {
            let client = Client::new();
            let consume_resp = client
                .get("http://localhost:1234/consume/test-topic/abc/1")
                .send()
                .await
                .unwrap();

            if consume_resp.status() == StatusCode::NO_CONTENT {
                if last_b_msg < 5 || last_a_msg < 5 {
                    assert!(
                        false,
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
                .post(format!("http://localhost:1234/ack/test-topic/abc/{msg_id}"))
                .send()
                .await
                .unwrap();

            assert_eq!(ack_resp.status(), StatusCode::OK);
        }
    });

    let mut last_a_msg = 0;
    let mut last_b_msg = 0;
    for _ in 0..15 {
        let client = Client::new();
        let consume_resp = client
            .get("http://localhost:1234/consume/test-topic/123/1")
            .send()
            .await
            .unwrap();

        if consume_resp.status() == StatusCode::NO_CONTENT {
            if last_b_msg < 5 || last_a_msg < 5 {
                assert!(
                    false,
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
