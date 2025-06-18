use axum::Json;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::{Router, http::StatusCode, routing::get};
use serde::Deserialize;
use serde_json::json;
use sled::{Config, Db, IVec};
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

    let shared_state = Arc::new(AppState {
        topic_db_map: topic_db_map,
    });

    // Build the application with a route
    let app = Router::new()
        .route("/health", get(health))
        .route(
            "/consume/{topic_name}/{consumer_id}/{batch_size}",
            get(consume_handler),
        )
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
    Path((topic_name, consumer_id, batch_size)): Path<(String, String, u16)>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let topic_db = match state.topic_db_map.get(&topic_name) {
        Some(db) => db,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Topic not found", "topic_name": topic_name })),
            );
        }
    };

    let consumer_state_db = match state.topic_db_map.get("consumer-state") {
        Some(db) => db,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "consumer-state db is missing"})),
            );
        }
    };

    let state_key = format!("{}-{}", topic_name, consumer_id);
    // todo - is there a get or insert function?
    let next_msg = match consumer_state_db.get(&state_key) {
        Ok(msg_id_opt) => match msg_id_opt {
            Some(msg_id) => msg_id,
            None => {
                println!(
                    "consumer-state db did not contain an entry for {}, setting to 0",
                    state_key
                );
                match consumer_state_db.insert(state_key, vec![0]) {
                    Ok(msg_id) => match msg_id {
                        Some(msg_id) => msg_id,
                        None => IVec::from(&[0]),
                    },
                    Err(error) => {
                        println!("error inserting into consumer state db: {}", error);
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({"error": "unable to insert into consumer state db"})),
                        );
                    }
                }
            }
        },
        Err(error) => {
            println!("error reading from consumer state db: {}", error);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "unable to read from consumer state db"})),
            );
        }
    };

    let mut results: Vec<IVec> = vec![];
    let range_iter = topic_db
        .range(next_msg..)
        .take(batch_size as usize)
        .filter_map(|e| match e {
            Ok(e) => Some(e),
            Err(err) => {
                print!(
                    "error reading messages from topic: {}, error: {}",
                    topic_name, err
                );
                None
            }
        });

    (StatusCode::OK, Json(json!("")))
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
