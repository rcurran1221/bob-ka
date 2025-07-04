use axum::Json;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Router, http::StatusCode, routing::get};
use serde::{Deserialize, Serialize};
use serde_json::{json, to_vec};
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
        Err(e) => panic!("unable to open consumer statedb: {e}"),
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
        .route("/produce/{topic_name}", post(produce_handler))
        .with_state(shared_state);

    // Run the server
    let addr = format!("0.0.0.0:{}", config.web_config.port);
    println!("listening at: {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}

async fn health() {}

async fn produce_handler(
    Path(topic_name): Path<String>,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<serde_json::Value>,
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

    let id = match topic_db.generate_id() {
        Ok(id) => id,
        Err(e) => {
            println!(
                "encountered error when trying to generate id for topic: {topic_name}, error: {e}"
            );
            // todo - struct for error responses
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("unable to generate id for topic: {}", topic_name)})),
            );
        }
    };

    let payload_as_bytes = match to_vec(&payload) {
        Ok(p) => p,
        Err(e) => {
            println!(
                "encountered error when converting payload to vec for topic: {topic_name}, error: {e}"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(
                    json!({"error": format!("unable to convert payload to bytes for topic: {}", topic_name)}),
                ),
            );
        }
    };

    match topic_db.insert(id.to_be_bytes(), payload_as_bytes) {
        Ok(_) => println!("successfully produced message: {id} for topic: {topic_name}"),
        Err(e) => {
            println!("error inserting payload: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("error inserting payload: {}", e)})),
            );
        }
    }

    (StatusCode::OK, Json(json!({"messageId": id})))
}

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

    let state_key = format!("{topic_name}-{consumer_id}");
    // todo - is there a get or insert function?
    let next_msg = match consumer_state_db.get(&state_key) {
        Ok(msg_id_opt) => match msg_id_opt {
            Some(msg_id) => msg_id,
            None => {
                println!(
                    "consumer-state db did not contain an entry for {state_key}, setting to 0"
                );
                match consumer_state_db.insert(state_key, vec![0]) {
                    Ok(msg_id) => match msg_id {
                        Some(msg_id) => msg_id,
                        None => IVec::from(&[0]),
                    },
                    Err(error) => {
                        println!("error inserting into consumer state db: {error}");
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({"error": "unable to insert into consumer state db"})),
                        );
                    }
                }
            }
        },
        Err(error) => {
            println!("error reading from consumer state db: {error}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "unable to read from consumer state db"})),
            );
        }
    };

    let messages: Vec<Message> = topic_db
        .range(next_msg..)
        .take(batch_size as usize)
        .filter_map(|e| match e {
            Ok(e) => {
                // if key from utf8 or value from utf8 err => return None
                let vec_as_array: [u8; 8] = match e.0.to_vec().try_into() {
                    Ok(v) => v,
                    Err(_) => {
                        println!("failed to convert vec into [u8; 8]");
                        return None;
                    }
                };

                let value = match String::from_utf8(e.1.to_vec()) {
                    Ok(value) => value,
                    Err(e) => {
                        println!("string from utf8 failed for key: {e}");
                        return None;
                    }
                };

                Some(Message {
                    msg_id: u64::from_be_bytes(vec_as_array),
                    message: value,
                })
            }
            Err(err) => {
                print!("error reading messages from topic: {topic_name}, error: {err}");
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
    pub web_config: WebServerConfig,
    pub topics: Vec<TopicConfig>,
}

#[derive(Debug, Deserialize)]
pub struct WebServerConfig {
    pub port: u16,
}

#[derive(Debug, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    pub compression: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct Message {
    msg_id: u64,
    message: String,
}

struct AppState {
    topic_db_map: HashMap<String, Db>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ErrorResponse {
    message: String,
}
