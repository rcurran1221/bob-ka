use axum::Json;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Router, http::StatusCode, routing::get};
use serde::{Deserialize, Serialize};
use serde_json::{json, to_vec};
use sled::{Config, Db, IVec, Tree};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
use tracing::{Level, event, info, span};

pub async fn start_web_server(config: BobConfig) -> Result<(), Box<dyn Error>> {
    let subscriber = tracing_subscriber::fmt()
        .with_thread_ids(true)
        .compact()
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    // todo - set up a log listener

    let span = span!(Level::INFO, "bob-ka");
    let _enter = span.enter();
    tracing::event!(Level::INFO, "starting web server...");

    let mut topic_db_map = HashMap::new();
    // iterate over topics, create dbs if they don't exist
    for topic in config.topics.iter() {
        info!("found topic: {}", topic.name);
        info!("enable compression: {}", topic.compression);
        info!("topic cap: {:?}", topic.cap);
        info!("topic_cap_tolerance: {:?}", topic.cap_tolerance);
        info!("backoff_dialation_ms: {:?}", topic.backoff_dialation_ms);

        if topic.name == "consumer_state" {
            panic!("cannot have a topic named consumer_state");
        }

        let topic_name = topic.name.clone();
        let topic_compression = topic.compression;
        let topic_temporary = topic.temporary;

        let sled_config = Config::new()
            .use_compression(topic.compression)
            .path(topic.name.clone())
            .temporary(topic.temporary);

        let db: Db = match sled_config.open() {
            Ok(db) => db,
            Err(e) => panic!("unable to open db: {}, error: {}", topic.name.clone(), e),
        };

        if topic_db_map.contains_key(&topic.name) {
            panic!("topic name: {} declared twice", topic.name)
        }

        let topic_tree = match db.open_tree("topic") {
            Ok(t) => t,
            Err(e) => panic!(
                "unable to open topic tree for topic:{}, error:{}",
                topic.name.clone(),
                e
            ),
        };

        let (rx, mut tx) = tokio::sync::mpsc::channel::<(String, u64)>(100);

        // one trim task per topic
        tokio::task::spawn({
            let topic_name = topic_name.clone();
            let topic_cap = topic.cap;
            let topic_cap_tolerance = topic.cap_tolerance;
            async move {
                loop {
                    match tx.recv().await {
                        Some(_) => {
                            let start = Instant::now();
                            trim_trail(
                                topic_name.clone(),
                                topic_cap,
                                topic_cap_tolerance,
                                topic_tree.clone(),
                            );
                            let duration = Instant::now().duration_since(start);
                            event!(
                                Level::INFO,
                                message = "succesfully trimmed topic tail",
                                duration = format!("{:?}", duration)
                            )
                        }
                        None => return, //channel is closed, pack it up
                    };
                }
            }
        });

        let stats_tree = match db.open_tree("stats") {
            Ok(t) => t,
            Err(e) => panic!(
                "unable to open stats tree for topic:{}, error:{}",
                topic.name.clone(),
                e
            ),
        };

        let topic_tree = match db.open_tree("topic") {
            Ok(t) => t,
            Err(e) => panic!(
                "unable to open topic tree for topic:{}, error:{}",
                topic.name.clone(),
                e
            ),
        };

        let bob_topic = BobTopic {
            stats_tree,
            topic_tree,
            top_db: db,
            topic_config: topic.clone(),
            event_sender: rx,
        };

        topic_db_map.insert(topic.name.clone(), bob_topic);

        event!(
            Level::INFO,
            message = "successfully created topic",
            topic_name = topic_name.clone(),
            topic_compression,
            topic_temporary
        )
    }

    event!(Level::INFO, "opening consumer state db");

    let consumer_state_config = Config::new()
        .path("consumer_state")
        .temporary(config.temp_consumer_state);

    let consumer_state_db: Db = match consumer_state_config.open() {
        Ok(db) => db,
        Err(e) => panic!("unable to open consumer statedb: {e}"),
    };

    event!(
        Level::INFO,
        message = "successfully opened consumer state db",
        config.temp_consumer_state
    );

    let shared_state = Arc::new(AppState {
        topic_db_map,
        consumer_state_db,
    });

    // Build the application with a route
    let app = Router::new()
        .route("/health", get(health))
        .route(
            "/consume/{topic_name}/{consumer_id}/{batch_size}",
            get(consume_handler),
        )
        .route(
            "/ack/{topic_name}/{consumer_id}/{last_msg_id}",
            post(ack_handler),
        )
        .route("/produce/{topic_name}", post(produce_handler))
        .route("/stats/{topic_name}", get(topic_stats_handler))
        .with_state(shared_state);

    // Run the server
    let addr = format!("0.0.0.0:{}", config.web_config.port);

    event!(
        Level::INFO,
        message = "web server is listening",
        address = addr
    );
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}

async fn health() {}

async fn topic_stats_handler(
    Path(topic_name): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    event!(Level::INFO, message = "got topic stats request", topic_name);

    match state.topic_db_map.get(&topic_name) {
        Some(topic) => (
            StatusCode::OK,
            Json(json!({"topic_name": topic_name, "topic_length" : topic.topic_tree.len()})),
        ),

        None => (
            StatusCode::NOT_FOUND,
            Json(json!("error: could not find topic: {topic_name}")),
        ),
    }
}

async fn ack_handler(
    Path((topic_name, consumer_id, ack_msg_id)): Path<(String, String, u64)>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    event!(
        Level::INFO,
        message = "received ack request",
        topic_name,
        consumer_id,
        ack_msg_id
    );

    let state_key = &format!("{topic_name}-{consumer_id}");
    let new_consumer_state = ack_msg_id + 1;

    let previous_consumer_state = match state
        .consumer_state_db
        .insert(state_key, IVec::from(&new_consumer_state.to_be_bytes()))
    {
        Ok(s) => match s {
            Some(s) => u64::from_be_bytes(s.to_vec().try_into().unwrap()),
            None => 0,
        },
        Err(e) => {
            println!(
                "received error when inserting consumer_state_db for key: {state_key}, error: {e}"
            );
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    event!(
        Level::INFO,
        message = "consumer state updated",
        previous_consumer_state,
        new_consumer_state,
        topic_name,
        consumer_id
    );

    StatusCode::OK
}

async fn produce_handler(
    Path(topic_name): Path<String>,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    let start = Instant::now();
    event!(Level::INFO, message = "got produce request", topic_name);

    let topic_db = match state.topic_db_map.get(&topic_name) {
        Some(db) => db,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Topic not found", "topic_name": topic_name })),
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
                Json(json!("error: unable to convert json to bytes")),
            );
        }
    };

    let id = match topic_db.top_db.generate_id() {
        Ok(id) => id,
        Err(e) => {
            println!("failed to generate id: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!("error: unable to generate id")),
            );
        }
    };

    let resp = match topic_db
        .topic_tree
        .insert(id.to_be_bytes(), payload_as_bytes)
    {
        Ok(_) => (StatusCode::OK, Json(json!({"messageId": id}))),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "error executing produce transaction" })),
        ),
    };

    let duration = Instant::now().duration_since(start);

    event!(
        Level::INFO,
        message = "successfully produced message",
        topic_name,
        id,
        duration = format!("{:?}", duration)
    );

    // notify for cleanup
    if topic_db
        .event_sender
        .send((topic_name.clone(), 1))
        .await
        .is_err()
    {
        event!(
            Level::ERROR,
            message = "failed to send on event_sender",
            topic_name
        )
    };

    resp
}

fn trim_trail(
    topic_name: String,
    topic_cap: Option<usize>,
    topic_cap_tolerance: Option<usize>,
    topic_tree: Tree,
) {
    let topic_length = topic_tree.len();

    event!(
        Level::INFO,
        message = "evaluating tail trim of topic",
        topic_name,
        topic_length,
        topic_cap,
        topic_cap_tolerance
    );

    if let Some(cap) = topic_cap
        && cap > 0
        && topic_length > (cap + topic_cap_tolerance.unwrap_or_default())
    {
        event!(
            Level::INFO,
            message = "topic out of tolerance, triming",
            topic_name,
            topic_length,
            cap,
            topic_cap_tolerance
        );

        let n_msgs = topic_length - cap;
        for _ in 0..n_msgs {
            topic_tree.pop_min().unwrap();
        }

        event!(
            Level::INFO,
            message = "succesfully trimmed topic",
            topic_name,
            cap,
        );
    }
}

async fn consume_handler(
    Path((topic_name, consumer_id, batch_size)): Path<(String, String, u16)>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let start = Instant::now();
    event!(
        Level::INFO,
        message = "got consume request",
        topic_name,
        consumer_id,
        batch_size
    );

    let topic_db = match state.topic_db_map.get(&topic_name) {
        Some(db) => db,
        None => {
            println!("topic not found: {topic_name}");
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Topic not found", "topic_name": topic_name })),
            );
        }
    };

    let state_key = format!("{topic_name}-{consumer_id}");
    let next_msg = match state.consumer_state_db.get(&state_key) {
        Ok(msg_id_opt) => match msg_id_opt {
            Some(msg_i) => msg_i,
            None => {
                println!(
                    "consumer-state db did not contain an entry for {state_key}, setting to 0"
                );
                IVec::from(&[0])
            }
        },
        Err(error) => {
            println!("error reading from consumer state db: {error}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Internal server error" })),
            );
        }
    };

    let events: Vec<Message> = topic_db
        .topic_tree
        .range(next_msg..)
        .take(batch_size as usize)
        .filter_map(|e| match e {
            Ok(e) => {
                // if key from utf8 or value from utf8 err => return None
                let key = match to_u64(e.0) {
                    Some(v) => v,
                    None => {
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
                    id: key,
                    data: serde_json::from_str(&value).unwrap(),
                })
            }
            Err(err) => {
                print!("error reading messages from topic: {topic_name}, error: {err}");
                None
            }
        })
        .collect();

    let n_events = events.len();
    let duration = Instant::now().duration_since(start);
    event!(
        Level::INFO,
        message = "successfully processed consume request",
        topic_name,
        consumer_id,
        n_events,
        duration = format!("{:?}", duration)
    );

    if n_events == 0 {
        if let Some(d) = topic_db.topic_config.backoff_dialation_ms {
            // sleep if no messages and backoff_dialation on
            sleep(Duration::from_millis(d)).await;
            event!(
                Level::INFO,
                message = "time dialated",
                sleep_ms = d,
                topic_name,
                consumer_id
            );
        }

        (StatusCode::NO_CONTENT, Json(json!({ "events": [] })))
    } else {
        (StatusCode::OK, Json(json!({ "events": events })))
    }
}

fn to_u64(input: IVec) -> Option<u64> {
    match input.to_vec().try_into() {
        Ok(i) => Some(u64::from_be_bytes(i)),
        Err(_) => None,
    }
}

#[derive(Debug, Deserialize)]
pub struct BobConfig {
    pub web_config: WebServerConfig,
    pub topics: Vec<TopicConfig>,
    pub temp_consumer_state: bool,
}

#[derive(Debug, Deserialize)]
pub struct WebServerConfig {
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TopicConfig {
    pub name: String,
    pub compression: bool,
    pub cap: Option<usize>,
    pub cap_tolerance: Option<usize>,
    pub temporary: bool,
    pub backoff_dialation_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Message {
    id: u64,
    data: serde_json::Value,
}

#[derive(Debug)]
struct AppState {
    topic_db_map: HashMap<String, BobTopic>,
    consumer_state_db: Db,
}

#[derive(Debug)]
pub struct BobTopic {
    pub top_db: Db,
    pub topic_tree: Tree,
    pub stats_tree: Tree,
    pub topic_config: TopicConfig,
    pub event_sender: Sender<(String, u64)>,
}
