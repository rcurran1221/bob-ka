use axum::extract::{Path, State};
use axum::{Router, routing::get};
use serde::Deserialize;
use sled::{Config, Db};
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use toml::de::from_str;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // load the old config up
    let config: BobConfig = match fs::read_to_string("config.toml") {
        Ok(content) => from_str(&content).expect("unable to parse config into struct"),
        Err(_) => panic!("cannot read config.toml!!"),
    };

    let mut topic_db_map = HashMap::new();
    // iterate over topics, create dbs if they don't exist
    for topic in config.topics.iter() {
        println!("found topic: {}", topic.name);
        println!("enable compression: {}", topic.compression);
        if topic.name == "consumer-state" {
            panic!("cannot have a topic named consumer-state");
        }
        let config = Config::new()
            .use_compression(topic.compression)
            .path(topic.name.clone());
        let db: Db = match config.open() {
            Ok(db) => db,
            Err(e) => panic!(
                "unable to open db: {}, error: {}",
                topic.name.clone(),
                e.to_string()
            ),
        };

        if topic_db_map.contains_key(&topic.name) {
            panic!("topic name: {} declared twice", topic.name)
        }
        topic_db_map.insert(topic.name.clone(), db);
    }

    println!("opening consumer state db");

    let db: Db = match sled::open("consumer-state") {
        Ok(db) => db,
        Err(e) => panic!("unable to open consumer statedb: {}", e.to_string()),
    };

    topic_db_map.insert("consumer-state".to_string(), db);

    // created topics

    let shared_state = Arc::new(AppState {
        topic_db_map: topic_db_map,
    });

    // Build the application with a route
    let app = Router::new()
        .route("/health", get(health))
        .route("/consume/{topic_name}/{consumer_id}", get(consume_handler))
        .with_state(shared_state);

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

async fn health() -> () {}

async fn consume_handler(
    Path((topic_name, consumer_id)): Path<(String, String)>,
    State(state): State<Arc<AppState>>,
) -> String {
    let topic_db = match state.topic_db_map.get(&topic_name) {
        Some(db) => db,
        None => return "test".to_string(), // todo - how to return a db not found code and error
                                           // string?
    };
    String::new()
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

struct AppState {
    topic_db_map: HashMap<String, Db>,
}
