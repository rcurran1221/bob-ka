use bob_ka::{BobConfig, TopicConfig, WebServerConfig};
use reqwest::{Client};

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

    // create http client and try to produce / consume?
}
