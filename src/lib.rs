use axum::Json;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::{Router, http::StatusCode, routing::get};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sled::{Config, Db, IVec};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

pub async fn start_web_server(config: BobConfig) -> Result<(), Box<dyn Error>> {
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
            Err(e) => panic!("unable to open db: {}, error: {}", topic.name.clone(), e),
        };

        if topic_db_map.contains_key(&topic.name) {
            panic!("topic name: {} declared twice", topic.name)
        }
        topic_db_map.insert(topic.name.clone(), db);
    }

    println!("opening consumer state db");

    let db: Db = match sled::open("consumer-state") {
        Ok(db) => db,
        Err(e) => panic!("unable to open consumer statedb: {}", e),
    };

    topic_db_map.insert("consumer-state".to_string(), db);

    let shared_state = Arc::new(AppState { topic_db_map });

    // Build the application with a route
    let app = Router::new()
        .route("/health", get(health))
        .route(
            "/consume/{topic_name}/{consumer_id}/{batch_size}",
            get(consume_handler),
        )
        .with_state(shared_state);

    // Run the server
    let addr = format!("0.0.0.0:{}", config.web_config.port);
    println!("listening at: {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}

async fn health() {}

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

    // array of key-value pairs eg. [{msg_id: 123, msg: {.. valid json obj}}]
    let messages: Vec<(String, String)> = topic_db
        .range(next_msg..)
        .take(batch_size as usize)
        .filter_map(|e| match e {
            Ok(e) => {
                // if key from utf8 or value from utf8 err => return None
                let key = match String::from_utf8(e.0.to_vec()) {
                    Ok(key) => key,
                    Err(e) => {
                        println!("string from utf8 failed for key: {}", e);
                        return None;
                    }
                };

                let value = match String::from_utf8(e.1.to_vec()) {
                    Ok(value) => value,
                    Err(e) => {
                        println!("string from utf8 failed for key: {}", e);
                        return None;
                    }
                };

                Some((key, value))
            }
            Err(err) => {
                print!(
                    "error reading messages from topic: {}, error: {}",
                    topic_name, err
                );
                None
            }
        })
        .collect();

    (
        StatusCode::OK,
        Json(json!(serde_json::to_string(&messages).unwrap())),
    )
}

#[derive(Debug, Deserialize)]
pub struct BobConfig {
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

#[derive(Debug, Deserialize, Serialize)]
struct Message {
    msg_id: u64,
    message: String,
}

struct AppState {
    topic_db_map: HashMap<String, Db>,
}
