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
use tracing::{Level, event, span};

// use tracing package?
// expose text/event-stream api?
// make level of tracing a config point
pub async fn start_web_server(config: BobConfig) -> Result<(), Box<dyn Error>> {
    let subscriber = tracing_subscriber::fmt().with_thread_ids(true).compact().finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let span = span!(Level::INFO, "start web server");
    let _enter = span.enter();
    event!(Level::INFO, "starting web server...");

    let mut topic_db_map = HashMap::new();
    // iterate over topics, create dbs if they don't exist
    for topic in config.topics.iter() {
        println!("found topic: {}", topic.name);
        println!("enable compression: {}", topic.compression);

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

        let stats_tree = match db.open_tree("stats") {
            Ok(t) => t,
            Err(e) => panic!(
                "unable to open stats tree for topic:{}, error:{}",
                topic.name.clone(),
                e
            ),
        };

        match stats_tree.insert("topic_cap", from_u64(topic.cap)) {
            Ok(_) => {}
            Err(e) => println!("failed to insert topic_cap: {e}"),
        };

        match stats_tree.insert("topic_cap_tolerance", from_u64(topic.cap_tolerance)) {
            Ok(_) => {}
            Err(e) => println!("failed to insert topic_cap_tolerance: {e}"),
        };

        let bob_topic = BobTopic {
            stats_tree,
            topic_tree,
            top_db: db,
        };

        topic_db_map.insert(topic.name.clone(), bob_topic);

        event!(
            Level::INFO,
            message = "successfully created topic",
            topic_name,
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

    drop(_enter); //exit config setup span

    let web_server_span = span!(Level::INFO, "web request listener");
    let _enter = web_server_span.enter();
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

async fn ack_handler(
    Path((topic_name, consumer_id, ack_msg_id)): Path<(String, String, u64)>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let span = span!(Level::INFO, "ack handler");
    let _enter = span.enter();
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
    let span = span!(Level::INFO, "produce handler");
    let _enter = span.enter();
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

    event!(
        Level::DEBUG,
        message = "successfully retrieved topic db",
        topic_name
    );

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

    event!(Level::DEBUG, message = "generating id", topic_name);
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

    event!(
        Level::DEBUG,
        message = "inserting payload into topic tree",
        topic_name,
        id
    );
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

    event!(
        Level::DEBUG,
        message = "updating topic length state",
        topic_name
    );
    let topic_length = match topic_db
        .stats_tree
        .update_and_fetch("topic_length", increment)
    {
        Ok(l) => match l {
            Some(l) => l,
            None => {
                println!("received none topic length after increment");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!("error: unable to get topic length")),
                );
            }
        },
        Err(e) => {
            println!("error getting topic length: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!("error: unable to get topic length")),
            );
        }
    };

    event!(Level::DEBUG, message = "getting topic cap", topic_name);
    let topic_cap = match topic_db.stats_tree.get("topic_cap") {
        Ok(c) => match c {
            Some(c) => to_u64(c),
            None => None,
        },
        Err(e) => {
            println!("unable to get topic cap for: {topic_name}, error: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!("error: error reading topic cap")),
            );
        }
    };

    event!(
        Level::DEBUG,
        message = "getting topic cap tolerance",
        topic_name
    );
    let topic_cap_tolerance = match topic_db.stats_tree.get("topic_cap_tolerance") {
        Ok(ct) => match ct {
            Some(c) => to_u64(c).unwrap_or_default(),
            None => 0,
        },
        Err(e) => {
            println!("unable to read topic cap tolerance for: {topic_name}, error: {e}");
            0
        }
    };

    // // lenght out of tolerance, trim n oldest, if topic_cap is some
    if let Some(cap) = topic_cap && cap > 0
        && let Some(topic_length) = to_u64(topic_length) // todo, probably want logging here
        && topic_length > (cap + topic_cap_tolerance)
    {
        event!(
            Level::DEBUG,
            message = "topic out of tolerance, triming",
            topic_name,
            topic_length,
            topic_cap_tolerance
        );

        let n_msgs = topic_length - cap;

        let n_oldest_items = topic_db
            .topic_tree
            .iter()
            .take((n_msgs) as usize)
            .filter_map(|item| item.ok());

        let mut batch = sled::Batch::default();
        for item in n_oldest_items {
            batch.remove(item.0);
        }

        if topic_db.topic_tree.apply_batch(batch).is_err() {
            println!("apply batch failed when triming")
        };

        event!(Level::DEBUG, message = "successfully trimmed topic", n_msgs);

        event!(
            Level::DEBUG,
            message = "decrementing topic length for trimmed items"
        );
        assert_eq!(topic_db.topic_tree.len() as u64, cap); // todo remove
        let topic_length = match topic_db
            .stats_tree
            .update_and_fetch("topic_length", |old| decrement(n_msgs, old))
        {
            Ok(l) => match l {
                Some(l) => l,
                None => {
                    println!("received none topic length after increment");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!("error: unable to get topic length")),
                    );
                }
            },
            Err(e) => {
                println!("error getting topic length: {e}");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!("error: unable to get topic length")),
                );
            }
        };

        event!(
            Level::DEBUG,
            message = "succesfully decremented topic state",
            topic_length = to_u64(topic_length).unwrap_or_default()
        );
    }

    event!(
        Level::INFO,
        message = "successfully produced message",
        topic_name,
        id
    );

    // return result from before triming
    resp
}

fn increment(old: Option<&[u8]>) -> Option<Vec<u8>> {
    let number = match old {
        Some(bytes) => {
            let array: [u8; 8] = bytes.try_into().unwrap();
            let number = u64::from_be_bytes(array);
            number + 1
        }
        None => 1,
    };

    Some(number.to_be_bytes().to_vec())
}

fn decrement(n: u64, old: Option<&[u8]>) -> Option<Vec<u8>> {
    let number = match old {
        Some(bytes) => {
            let array: [u8; 8] = bytes.try_into().unwrap();
            let number = u64::from_be_bytes(array);
            number - n
        }
        None => 0,
    };

    Some(number.to_be_bytes().to_vec())
}

async fn consume_handler(
    Path((topic_name, consumer_id, batch_size)): Path<(String, String, u16)>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let span = span!(Level::INFO, "consume handler");
    let _enter = span.enter();
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
                    id: u64::from_be_bytes(vec_as_array),
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
    event!(
        Level::INFO,
        message = "successfully processed consume request",
        topic_name,
        consumer_id,
        n_events
    );

    // if n_events is zero, hold response for N seconds?
    // or take a token, allow for N requests resulting in no events in X seconds
    if n_events == 0 {
        (StatusCode::NO_CONTENT, Json(json!({ "events": [] })))
    } else {
        (StatusCode::OK, Json(json!({ "events": events })))
    }
}

// todo - use these in program
// maybe extend From implementation of IVec and u64?
fn from_u64(input: u64) -> IVec {
    IVec::from(&input.to_be_bytes())
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

#[derive(Debug, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    pub compression: bool,
    pub cap: u64,
    pub cap_tolerance: u64,
    pub temporary: bool,
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

#[derive(Debug, Deserialize, Serialize)]
struct ErrorResponse {
    message: String,
}

#[derive(Debug)]
pub struct BobTopic {
    pub top_db: Db,
    pub topic_tree: Tree,
    pub stats_tree: Tree,
}
