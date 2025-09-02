use axum::Json;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Router, http::StatusCode, routing::get};
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use serde_json::{json, to_vec};
use sled::{Config, Db, IVec, Tree};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
use tower_http::services::ServeDir;
use tracing::{Level, event, info, span};
use tracing_appender::rolling;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

pub async fn start_web_server(config: BobConfig) -> Result<(), Box<dyn Error>> {
    let file_appender = rolling::daily("logs", "bob_ka.log");

    let stdout_layer = fmt::layer().with_target(false).with_level(true);

    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_target(false)
        .with_level(true)
        .with_thread_ids(true)
        .with_writer(file_appender);

    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(file_layer)
        .with(LevelFilter::from_level(Level::INFO))
        .init();

    // Combine layers and initialize the subscriber
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

        if topic_db_map.contains_key(&topic.name.to_lowercase()) {
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
        let timestamp_tree = match db.open_tree("timestamp") {
            Ok(t) => t,
            Err(e) => panic!(
                "unable to open timestamp tree for topic: {}, error: {}",
                topic.name.clone(),
                e
            ),
        };

        // set up post produce events
        let (rx, mut tx) = tokio::sync::mpsc::channel::<(String, u64)>(100);

        // one trim task per topic
        tokio::task::spawn({
            let topic_cap = topic.cap;
            let topic_cap_tolerance = topic.cap_tolerance;
            let topic_time_retention = topic.time_based_retention;
            async move {
                loop {
                    match tx.recv().await {
                        Some((topic_name, _)) => {
                            let start = Instant::now();
                            match topic_time_retention {
                                Some(t) => {
                                    trim_tail_time(
                                        topic_name,
                                        t,
                                        topic_tree.clone(),
                                        timestamp_tree.clone(),
                                    );
                                }
                                None => {
                                    trim_trail(
                                        topic_name,
                                        topic_cap,
                                        topic_cap_tolerance,
                                        topic_tree.clone(),
                                    );
                                }
                            }

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

        let timestamp_tree = match db.open_tree("timestamp") {
            Ok(t) => t,
            Err(e) => panic!(
                "unable to open timestamp tree for topic: {}, error: {}",
                topic.name.clone(),
                e
            ),
        };
        let bob_topic = BobTopic {
            stats_tree,
            topic_tree,
            top_db: db,
            timestamp_tree,
            topic_config: topic.clone(),
            event_sender: rx,
        };

        topic_db_map.insert(topic.name.clone().to_lowercase(), bob_topic);

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
        .route("/peek/{topic_name}/{n_items}", get(peek_handler))
        .route("/all_topics", get(all_topics))
        .nest_service("/static", ServeDir::new("./static"))
        .with_state(shared_state);

    // Run the server
    let addr = format!("0.0.0.0:{}", config.web_config.port);

    event!(
        Level::INFO,
        message = "web server is listening",
        address = addr
    );

    // ok to unwrap here
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}

async fn health() {}

async fn all_topics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    event!(Level::INFO, message = "got get all topic names request");

    let topic_names: Vec<String> = state.topic_db_map.keys().map(|k| k.clone()).collect();

    if topic_names.len() == 0 {
        (StatusCode::NO_CONTENT, Json(json!({})))
    } else {
        (StatusCode::OK, Json(json!({"topic_names": topic_names})))
    }
}

async fn peek_handler(
    Path((topic_name, n)): Path<(String, usize)>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    event!(Level::INFO, message = "got peek request", topic_name);

    let topic_db = match state.topic_db_map.get(&topic_name.to_lowercase()) {
        Some(topic) => topic,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "could not find topic: {topic_name}"})),
            );
        }
    };

    let results = topic_db
        .topic_tree
        .iter()
        .rev()
        .take(n)
        .filter_map(|e| match e {
            Ok(e) => test(e, topic_name.clone()),
            Err(e) => {
                event!(
                    Level::ERROR,
                    message = "error reading message from topic db",
                    error = e.to_string()
                );
                None
            }
        })
        .collect::<Vec<OutgoingMessage>>();

    event!(Level::INFO, message = "responding to peek request");

    if results.len() == 0 {
        (StatusCode::NO_CONTENT, Json(json!({ "events": [] })))
    } else {
        (StatusCode::OK, Json(json!({ "events": results})))
    }
}

async fn topic_stats_handler(
    Path(topic_name): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    event!(Level::INFO, message = "got topic stats request", topic_name);

    match state.topic_db_map.get(&topic_name.to_lowercase()) {
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
    let start = Instant::now();
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
            Some(s) => to_u64(s).unwrap_or_default(),
            None => 0,
        },
        Err(e) => {
            println!(
                "received error when inserting consumer_state_db for key: {state_key}, error: {e}"
            );
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    let duration = Instant::now().duration_since(start);

    event!(
        Level::INFO,
        message = "consumer state updated",
        previous_consumer_state,
        new_consumer_state,
        topic_name,
        consumer_id,
        duration = format!("{:?}", duration),
    );

    StatusCode::OK
}

async fn produce_handler(
    Path(topic_name): Path<String>,
    State(state): State<Arc<AppState>>,
    Json(incoming_message): Json<serde_json::Value>,
) -> impl IntoResponse {
    let start = Instant::now();
    event!(Level::INFO, message = "got produce request", topic_name);

    let topic_db = match state.topic_db_map.get(&topic_name.to_lowercase()) {
        Some(db) => db,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Topic not found", "topic_name": topic_name })),
            );
        }
    };

    let message = AtRestMessage {
        data: incoming_message,
        timestamp: Local::now(),
    };

    let message_as_bytes = match to_vec(&message) {
        Ok(p) => p,
        Err(e) => {
            println!(
                "encountered error when converting incoming message to vec for topic: {topic_name}, error: {e}"
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
        .insert(id.to_be_bytes(), message_as_bytes)
    {
        Ok(_) => (StatusCode::OK, Json(json!({"messageId": id}))),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "error executing produce transaction" })),
        ),
    };

    let unix_timestamp = get_unix_timestamp();

    if topic_db
        .timestamp_tree
        .insert(IVec::from(&unix_timestamp.to_be_bytes()), &id.to_be_bytes())
        .is_err()
    {
        event!(Level::ERROR, message = "failed to insert into timestamp db",);
    }

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

fn get_unix_timestamp() -> u64 {
    let now = SystemTime::now();
    now.duration_since(UNIX_EPOCH).unwrap().as_secs()
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
            if topic_tree.pop_min().is_err() {
                event!(Level::ERROR, message = "error poping min", topic_name)
            }
        }

        event!(
            Level::INFO,
            message = "succesfully trimmed topic",
            topic_name,
            cap,
        );
    }
}

fn trim_tail_time(
    topic_name: String,
    topic_retention_time: usize,
    topic_tree: Tree,
    timestamp_tree: Tree,
) {
    event!(
        Level::INFO,
        message = "triming tail of topic by time",
        topic_name,
        topic_retention_time,
    );

    let delete_before_timestamp = SystemTime::now()
        .checked_sub(Duration::from_secs((topic_retention_time * 60) as u64))
        .unwrap_or(UNIX_EPOCH)
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_be_bytes();

    let delete_before_id = timestamp_tree
        .get(delete_before_timestamp)
        .unwrap_or_else(|_| {
            event!(
                Level::ERROR,
                message = "failed to get delete id from timestamp",
                topic_name,
            );
            Some(IVec::default())
        })
        .unwrap_or_default();

    let mut count_delete = 0;
    for item in topic_tree.range(..delete_before_id) {
        topic_tree
            .remove(item.unwrap_or_default().0)
            .unwrap_or_default();
        count_delete += 1;
    }
    for item in timestamp_tree.range(..delete_before_timestamp) {
        timestamp_tree
            .remove(item.unwrap_or_default().0)
            .unwrap_or_else(|_| {
                event!(
                    Level::ERROR,
                    message = "failed to remove from timestamp tree",
                    topic_name
                );
                Some(IVec::default())
            });
    }

    event!(
        Level::INFO,
        message = "successfully trimmed tail by time",
        topic_name,
        topic_retention_time,
        count_delete
    );
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

    let topic_db = match state.topic_db_map.get(&topic_name.to_lowercase()) {
        Some(db) => db,
        None => {
            event!(Level::ERROR, message = "topic not found", topic_name);
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
                event!(
                    Level::INFO,
                    message =
                        "consumer-state db did not contain an entry for {state_key}, setting to 0"
                );
                IVec::from(&[0])
            }
        },
        Err(error) => {
            event!(
                Level::ERROR,
                message = "error reading from consumer state db"
            );
            println!("error reading from consumer state db: {error}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Internal server error" })),
            );
        }
    };

    let events: Vec<OutgoingMessage> = topic_db
        .topic_tree
        .range(next_msg..)
        .take(batch_size as usize)
        .filter_map(|e| match e {
            Ok(e) => {
                let key = match to_u64(e.0) {
                    Some(v) => v,
                    None => {
                        event!(
                            Level::ERROR,
                            message = "failed to convert vec into [u8; 8]",
                            topic_name
                        );
                        return None;
                    }
                };

                let value = match String::from_utf8(e.1.to_vec()) {
                    Ok(value) => value,
                    Err(_) => {
                        event!(
                            Level::ERROR,
                            message = "ivec to string from utf8 failed",
                            topic_name
                        );
                        return None;
                    }
                };

                let message: AtRestMessage = match serde_json::from_str(&value) {
                    Ok(m) => m,
                    Err(e) => {
                        event!(
                            Level::ERROR,
                            message = "failed to parse value into message",
                            error = e.to_string(),
                            json = value,
                        );
                        return None;
                    }
                };

                Some(OutgoingMessage {
                    id: key,
                    data: message.data,
                    timestamp: message.timestamp,
                })
            }
            Err(_) => {
                event!(
                    Level::ERROR,
                    message = "error reading messages from topic",
                    topic_name
                );
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

fn test(e: (IVec, IVec), topic_name: String) -> Option<OutgoingMessage> {
    let key = match to_u64(e.0) {
        Some(v) => v,
        None => {
            event!(
                Level::ERROR,
                message = "failed to convert vec into [u8; 8]",
                topic_name
            );
            return None;
        }
    };

    let value = match String::from_utf8(e.1.to_vec()) {
        Ok(value) => value,
        Err(_) => {
            event!(
                Level::ERROR,
                message = "ivec to string from utf8 failed",
                topic_name
            );
            return None;
        }
    };

    let message: AtRestMessage = match serde_json::from_str(&value) {
        Ok(m) => m,
        Err(e) => {
            event!(
                Level::ERROR,
                message = "failed to parse value into message",
                error = e.to_string(),
                json = value,
            );
            return None;
        }
    };

    Some(OutgoingMessage {
        id: key,
        data: message.data,
        timestamp: message.timestamp,
    })
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
    pub time_based_retention: Option<usize>, // time in minutes to retain
    pub cap: Option<usize>,
    pub cap_tolerance: Option<usize>,
    pub temporary: bool,
    pub backoff_dialation_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct OutgoingMessage {
    id: u64,
    data: serde_json::Value,
    timestamp: DateTime<Local>,
}

#[derive(Debug, Deserialize, Serialize)]
struct AtRestMessage {
    data: serde_json::Value,
    timestamp: DateTime<Local>,
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
    pub timestamp_tree: Tree,
    pub topic_config: TopicConfig,
    pub event_sender: Sender<(String, u64)>,
}
