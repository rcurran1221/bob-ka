use axum::Json;
use axum::extract::{ConnectInfo, Path, State};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Router, http::StatusCode, routing::get};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, to_vec};
use sled::{Config, Db, IVec, Tree};
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
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

    let node_id = uuid::Uuid::new_v4().hyphenated().to_string();

    event!(Level::INFO, message = "node id generated", node_id);

    let mut topic_db_map = HashMap::new();
    // iterate over topics, create dbs if they don't exist
    if let Some(topics) = config.topics {
        for topic in topics.iter() {
            info!("found topic: {}", topic.name);
            info!("enable compression: {}", topic.compression);
            info!("topic cap: {:?}", topic.cap);
            info!("topic_cap_tolerance: {:?}", topic.cap_tolerance);
            info!("backoff_dialation_ms: {:?}", topic.backoff_dialation_ms);

            if topic.name == "consumer_state" {
                panic!("cannot have a topic named consumer_state");
            }

            if let Some(addr) = topic.mothership_address.clone() {
                tokio::task::spawn({
                    let topic_name = topic.name.clone();
                    let node_id = node_id.clone();
                    let node_port = config.web_config.port.clone();
                    async move {
                        event!(
                            Level::INFO,
                            message = "this node will phone home to mothership",
                            topic_name = topic_name,
                            address = addr,
                            node_id,
                        );
                        let client = reqwest::Client::new();

                        loop {
                            let should_retry = match client
                                .post(format!("{addr}/register"))
                                .json(&json!({ "node_id": node_id , "topic_name": topic_name,
                                     "node_port": node_port}))
                                .send()
                                .await
                            {
                                Ok(resp) => {
                                    if resp.status() != StatusCode::OK {
                                        event!(
                                            Level::ERROR,
                                            message = "call home failed",
                                            status_code = resp.status().to_string(),
                                        );
                                        true
                                    } else {
                                        event!(
                                            Level::INFO,
                                            message = "successfully called home",
                                            address = addr,
                                            node_id = node_id
                                        );
                                        false
                                    }
                                }
                                Err(e) => {
                                    event!(
                                        Level::ERROR,
                                        message = "failed to send request home",
                                        error = e.to_string()
                                    );
                                    true
                                }
                            };

                            if !should_retry {
                                break;
                            }
                            event!(
                                Level::WARN,
                                message = "retrying call to mothership...",
                                node_id,
                            );

                            sleep(Duration::from_millis(1000)).await;
                        }
                    }
                });
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

            topic_db_map.insert(topic.name.clone(), bob_topic);

            event!(
                Level::INFO,
                message = "successfully created topic",
                topic_name = topic_name.clone(),
                topic_compression,
                topic_temporary
            )
        }
    }

    // dont do this if mothership, this means consume_state db becomes option on app state
    let consumer_state_db = match config.mothership {
        Some(_) => None,
        None => {
            event!(Level::INFO, "opening consumer state db");

            let consumer_state_config = Config::new().path("consumer_state");

            let consumer_state_db: Db = match consumer_state_config.open() {
                Ok(db) => db,
                Err(e) => panic!("unable to open consumer statedb: {e}"),
            };

            event!(
                Level::INFO,
                message = "successfully opened consumer state db",
            );

            Some(consumer_state_db)
        }
    };

    let mothership_db = match config.mothership {
        Some(_) => {
            let mothership_db = match Config::new().path("mothership_db").open() {
                Ok(db) => db,
                Err(e) => panic!("unable to open mothership db: {e}"),
            };

            Some(mothership_db)
        }
        None => None,
    };

    let shared_state = Arc::new(AppState {
        topic_db_map,
        consumer_state_db,
        mothership_db,
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
        .route("/register", post(register_node_handler)) // todo - how to conditionally add route
        .with_state(shared_state)
        .into_make_service_with_connect_info::<SocketAddr>();

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

#[derive(Serialize, Deserialize)]
struct RegisterRequest {
    topic_name: String,
    node_id: String,
    node_port: usize,
}

async fn register_node_handler(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<AppState>>,
    Json(request): Json<RegisterRequest>,
) -> impl IntoResponse {
    let node_address = format!("{}:{}", addr.ip(), request.node_port); // ip:port i believe

    let topic_name = request.topic_name;
    let node_id = request.node_id;

    match state.mothership_db.clone() {
        None => {
            event!(
                Level::INFO,
                message =
                    "mothership db does not exist, should not have received a request to this url"
            );
            return StatusCode::BAD_REQUEST;
        }
        Some(db) => {
            let data = format!("{node_address}|{node_id}");
            match db.insert(topic_name.clone().into_bytes(), data.into_bytes()) {
                Ok(_) => {}
                Err(_) => {
                    event!(
                        Level::ERROR,
                        message = "failed to insert into mothership db",
                        topic_name = topic_name.clone(),
                    );
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            };
        }
    }

    event!(
        Level::INFO,
        message = "successfully registered node with mothership",
        node_id,
        node_address,
        topic_name,
    );

    StatusCode::OK
}

fn get_topic_node_info(
    topic_name: String,
    mothership_db: Db,
) -> Result<(String, String), (StatusCode, Json<Value>)> {
    let node_data = match mothership_db.get(topic_name.clone().into_bytes()) {
        Ok(o) => match o {
            Some(node_data) => match to_string(node_data) {
                Some(node_data) => node_data,
                None => {
                    return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(json!({}))));
                }
            },
            None => return Err((StatusCode::BAD_REQUEST, Json(json!({})))),
        },
        Err(_) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(json!({}))));
        }
    };

    let mut data_split = node_data.split("|");
    let node_address = match data_split.next() {
        Some(addr) => addr,
        None => {
            event!(
                Level::ERROR,
                message = "bad data in mothership entry",
                topic_name,
                node_data,
            );
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "bad data for mothership entry"})),
            ));
        }
    };
    let node_id = match data_split.next() {
        Some(id) => id,
        None => {
            event!(
                Level::ERROR,
                message = "bad data in mothership entry",
                topic_name,
                node_data,
            );
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "bad data for mothership entry"})),
            ));
        }
    };
    Ok((node_address.to_string(), node_id.to_string()))
}

async fn topic_stats_handler(
    Path(topic_name): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // how to differentiate logs between mothership and child nodes?
    event!(Level::INFO, message = "got topic stats request", topic_name);

    if let Some(db) = state.mothership_db.clone() {
        let (node_address, node_id) = match get_topic_node_info(topic_name.clone(), db) {
            Ok((addr, id)) => (addr, id),
            Err(e) => return e, // http error back to caller
        };

        async fn forward_to_child(
            topic_name: String,
            node_address: String,
            node_id: String,
            url_path: String,
        ) -> (StatusCode, Json<Value>) {
            let client = reqwest::Client::new(); // client re-use per child node for tcp connection caching
            let topic_name = topic_name.clone();
            let url = format!("http://{node_address}{url_path}"); // todo - make "child node https" a config point
            match client.get(url).send().await {
                Err(e) => {
                    event!(
                        Level::ERROR,
                        message = "failed to contact child node",
                        node_address,
                        topic_name,
                        node_id,
                        error = e.to_string(),
                    );
                    return (StatusCode::BAD_GATEWAY, Json(json!({})));
                }
                Ok(resp) => {
                    let status_code = resp.status();
                    let resp_text = match resp.text().await {
                        Err(e) => {
                            event!(
                                Level::ERROR,
                                message = "could not read text on response",
                                node_address,
                                topic_name,
                                node_id,
                                error = e.to_string(),
                            );
                            return (StatusCode::BAD_GATEWAY, Json(json!({})));
                        }
                        Ok(t) => t,
                    };

                    match serde_json::from_str(&resp_text) {
                        Err(e) => {
                            event!(
                                Level::ERROR,
                                message = "unable to convert resp to json value",
                                error = e.to_string(),
                            );
                            return (StatusCode::BAD_GATEWAY, Json(json!({})));
                        }
                        Ok(r) => return (status_code, Json(r)),
                    };
                }
            };
        }

        let url_path = format!("/stats/{topic_name}");
        return forward_to_child(topic_name, node_address, node_id, url_path).await;
    }

    // non-mothership path
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
        .clone()
        .unwrap() // todo
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

    let topic_db = match state.topic_db_map.get(&topic_name) {
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
    let next_msg = match state.consumer_state_db.clone().unwrap().get(&state_key) {
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

    let events: Vec<Message> = topic_db
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

                Some(Message {
                    id: key,
                    data: serde_json::from_str(&value).unwrap_or_default(),
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

fn to_u64(input: IVec) -> Option<u64> {
    match input.to_vec().try_into() {
        Ok(i) => Some(u64::from_be_bytes(i)),
        Err(_) => None,
    }
}

fn to_string(input: IVec) -> Option<String> {
    match String::from_utf8(input.to_vec()) {
        Ok(s) => Some(s),
        Err(_) => None,
    }
}

#[derive(Debug, Deserialize)]
pub struct BobConfig {
    pub web_config: WebServerConfig,
    pub topics: Option<Vec<TopicConfig>>,
    pub mothership: Option<MothershipConfig>,
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
    pub mothership_address: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MothershipConfig {
    pub is_mothership: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct Message {
    id: u64,
    data: serde_json::Value,
}

#[derive(Debug)]
struct AppState {
    topic_db_map: HashMap<String, BobTopic>,
    consumer_state_db: Option<Db>,
    mothership_db: Option<Db>,
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
