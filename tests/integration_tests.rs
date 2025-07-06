use bob_ka::{BobConfig, TopicConfig, WebServerConfig};
use hyper::StatusCode;
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[tokio::test]
async fn test() {
    // start web server on background task
    tokio::task::spawn(async {
        bob_ka::start_web_server(BobConfig {
            web_config: WebServerConfig { port: 1234 },
            topics: vec![TopicConfig {
                name: "test-topic".to_string(),
                compression: true,
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
                })
                .send()
                .await
                .unwrap();

            assert_eq!(produce_resp.status(), StatusCode::OK);
        }
    });

    for i in 0..5 {
        let client = Client::new();
        let consume_resp = client
            .get("http://localhost:1234/consume/test-topic/123/1")
            .send()
            .await
            .unwrap();

        assert_eq!(consume_resp.status(), StatusCode::OK);
        let resp_body = consume_resp.text().await.unwrap();
        println!("{resp_body}");
        let msgs: Vec<Event> = serde_json::from_str(&resp_body).unwrap();
        assert_eq!(msgs.len(), 1);
        // todo - fix this assert, could be a or b
        // assert_eq!(msgs[0].message.event_name, format!("{i}"));
        let msg_id = msgs[0].msg_id;

        // ack messgae
        let ack_resp = client
            .post(format!("http://localhost:1234/ack/test-topic/123/{msg_id}"))
            .send()
            .await
            .unwrap();

        assert_eq!(ack_resp.status(), StatusCode::OK);
    }
}

// todo - concurrent topic production, multiple consumers
// assert  messages produced within single process stay in order
// assert all consumers get all messages in expected order
// multiple topics

#[derive(Serialize, Deserialize)]
struct Message {
    event_data: String,
    event_name: String,
}

#[derive(Serialize, Deserialize)]
struct Event {
    msg_id: u64,
    message: Message,
}
