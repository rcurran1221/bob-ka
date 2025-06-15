use axum::{Router, routing::get};
use serde::Deserialize;
use std::fs;
use std::net::SocketAddr;
use toml::de::from_str;
use std::error::Error;
use sled::{Db, Config};
use std::collections::HashMap;

#[tokio::main]
async fn main()  -> Result<(), Box<dyn Error>>{
    // load the old config up
    let config : BobConfig = match fs::read_to_string("config.toml") {
        Ok(content) => from_str(&content).expect("unable to parse config into struct"),
        Err(_) => panic!("cannot read config.toml!!"),
    };

    let mut topic_db_map = HashMap::new();
    // iterate over topics, create dbs if they don't exist
    for topic in config.topics.iter() {
        println!("found topic: {}", topic.name);
        println!("enable compression: {}", topic.compression);
        let config = Config::new().use_compression(topic.compression).path(topic.name.clone());
        let db : Db = match config.open() {
            Ok(db) => db,
            Err(_) => panic!("unable to open db: {}", topic.name.clone())
        };

        if topic_db_map.contains_key(&topic.name) {
            panic!("topic name: {} declared twice", topic.name)
        }
        topic_db_map.insert(topic.name.clone(), db);
    }

    // Build the application with a route
    let app = Router::new().route("/", get(hello));

    // Define the address to bind the server
    let addr = SocketAddr::from(([127, 0, 0, 1], config.web_config.port));
    println!("Listening on http://{}", addr);

    // Run the server
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();

    Ok(())
}

async fn hello() -> &'static str {
    "Hello, world!"
}
#[derive(Debug, Deserialize)]
struct BobConfig {
    web_config: WebServerConfig,
    topics: Vec<TopicConfig>,
}

#[derive(Debug, Deserialize)]
struct WebServerConfig {
    port: u16,
}

#[derive(Debug, Deserialize)]
struct TopicConfig {
    name: String,
    compression: bool,
}
