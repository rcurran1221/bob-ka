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

    let client = Client::new();
    // produce n messages
    for i in 0..5 {
        let produce_resp = client
            .post("http://localhost:1234/produce/test-topic")
            .json(&Message {
                event_name: format!("event{i}").to_string(),
                event_data: "this is data".to_string(),
            })
            .send()
            .await
            .unwrap();

        assert_eq!(produce_resp.status(), StatusCode::OK);
    }

    // consume n messages in batches of m, ack in between

    let consume_resp = client.get("http://localhost:1234/consume/test-topic/123/1").send().await.unwrap();

    assert_eq!(consume_resp.status(), StatusCode::OK);
    let resp_body = consume_resp.text().await.unwrap();
    print!("{resp_body}");

    // assert messages are read in order
}

#[derive(Serialize, Deserialize)]
struct Message {
    event_name: String,
    event_data: String,
}
