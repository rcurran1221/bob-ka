use axum::Json;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Router, http::StatusCode, routing::get};
use serde::{Deserialize, Serialize};
use serde_json::{json, to_vec};
use sled::transaction::ConflictableTransactionError;
use sled::{transaction, Config, Db, IVec, Transactional, Tree};
use std::collections::HashMap;
use std::error::{self, Error};
use std::sync::Arc;
use std::u64;

// how to document apis? solidfy
// retention policy implementenation
//  keep track of number of records as kvp, trim when exceeding that length
//  need to use a merge operator? this means seperate "tree" for topic length and other stats?
// use tracing package?
// expose text/event-stream api?
// implement ivec to and from u64 functions
pub async fn start_web_server(config: BobConfig) -> Result<(), Box<dyn Error>> {
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
            topic_tree,
            stats_tree,
            top_db: db,
        };

        topic_db_map.insert(topic.name.clone(), bob_topic);

        println!(
            "successfully created db for topic: {topic_name}, compression: {topic_compression}, temporary: {topic_temporary} "
        )
    }

    println!("opening consumer state db");

    let consumer_state_db: Db = match sled::open("consumer_state") {
        Ok(db) => db,
        Err(e) => panic!("unable to open consumer statedb: {e}"),
    };

    let recovered = consumer_state_db.was_recovered();
    println!("consumer_state_db recovered: {recovered}");

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
        .with_state(shared_state);

    // Run the server
    let addr = format!("0.0.0.0:{}", config.web_config.port);
    println!("listening at: {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}

async fn health() {}

async fn ack_handler(
    Path((topic_name, consumer_id, ack_msg_id)): Path<(String, String, u64)>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
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

    println!(
        "consumer state for {state_key} updated from {previous_consumer_state} to {new_consumer_state}"
    );

    StatusCode::OK
}

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


    // start trans

    // match topic_db
    //     .topic_tree
    //     .insert(id.to_be_bytes(), payload_as_bytes)
    // {
    //     Ok(_) => println!("successfully produced message: {id} for topic: {topic_name}"),
    //     Err(e) => {
    //         println!("error inserting payload: {e}");
    //         return (
    //             StatusCode::INTERNAL_SERVER_ERROR,
    //             Json(json!({"error": format!("error inserting payload: {}", e)})),
    //         );
    //     }
    // }
    //
    // let topic_length = match topic_db
    //     .stats_tree
    //     .update_and_fetch("topic_length", increment)
    // {
    //     Ok(l) => match l {
    //         Some(l) => u64::from_be_bytes(l.to_vec().try_into().unwrap()),
    //         None => 0,
    //     },
    //     Err(e) => {
    //         println!("error reading topic_length for {topic_name}: {e}");
    //         return (
    //             StatusCode::INTERNAL_SERVER_ERROR,
    //             Json(json!({"error": format!("error reading topic_length")})),
    //         );
    //     }
    // };
    //
    // let topic_cap = match topic_db.stats_tree.get("topic_cap") {
    //     Ok(c) => match c {
    //         Some(c) => match to_u64(c) {
    //             Some(c) => c,
    //             None => 0,
    //         },
    //         None => 0,
    //     },
    //     Err(e) => {
    //         println!("failed to read topic_cap: {}", e);
    //         return (
    //             StatusCode::INTERNAL_SERVER_ERROR,
    //             Json(json!({"error": format!("error reading topic_catp")})),
    //         );
    //     }
    // };
    //
    // let topic_cap_tolerance = match topic_db.stats_tree.get("topic_cap_tolerance") {
    //     Ok(c) => match c {
    //         Some(c) => match to_u64(c) {
    //             Some(c) => c,
    //             None => 0,
    //         },
    //         None => 0,
    //     },
    //     Err(e) => {
    //         println!("failed to read topic_cap_tolerance: {}", e);
    //         return (
    //             StatusCode::INTERNAL_SERVER_ERROR,
    //             Json(json!({"error": format!("error reading topic_cap_tolerance")})),
    //         );
    //     }
    // };
    //
    // if topic_length > (topic_cap + topic_cap_tolerance) {
    //     topic_db
    //         .topic_tree
    //         .range::<&[u8], _>(..)
    //         .take((topic_length - topic_cap) as usize)
    //         .for_each(|item| {
    //             let item = match item {
    //                 Ok(i) => i,
    //                 Err(_) => {
    //                     println!("failed to read msg for topic: {topic_name}");
    //                     // what does this return to?
    //                     return;
    //                 }
    //             };
    //             if topic_db.topic_tree.remove(item.0).is_err() {
    //                 println!("failed to remove item from topic: {topic_name}");
    //             }
    //         });
    // }

    // todo - figure out tran
    let transaction_result = (&topic_db.topic_tree, &topic_db.stats_tree).transaction(|(topic, stats)| {
        let payload_as_bytes = match to_vec(&payload) {
            Ok(p) => p,
            Err(e) => {
                println!(
                    "encountered error when converting payload to vec for topic: {topic_name}, error: {e}"
                );
                return Err(ConflictableTransactionError::Abort(()));
            }
        };

        let id = match topic.generate_id() {
            Ok(id) => id,
            Err(e) => {
                println!(
                    "encountered error when trying to generate id for topic: {topic_name}, error: {e}"
                );
                return Err(ConflictableTransactionError::Abort(()));
            }
        };

        topic_db
            .topic_tree
            .insert(id.to_be_bytes(), payload_as_bytes)?;

        let topic_length = match stats
            .update_and_fetch("topic_length", increment)?
        {
            Some(l) => u64::from_be_bytes(l.to_vec().try_into().unwrap()),
            None => 0,
        };

        let topic_cap = match topic_db.stats_tree.get("topic_cap")? {
            Some(c) => to_u64(c),
            None => None,
        };

        let topic_cap_tolerance = match topic_db.stats_tree.get("topic_cap_tolerance")? {
            Some(c) => to_u64(c),
            None => None,
        };

        if let Some(cap) = topic_cap
            && let Some(tolerance) = topic_cap_tolerance
            && topic_length > (cap + tolerance)
        {
            for item in topic_db
                .topic_tree
                .range::<&[u8], _>(..)
                .take((topic_length - cap) as usize)
            {
                match item {
                    Ok(i) => {
                        if topic_db.topic_tree.remove(i.0).is_err() {
                            println!(
                                "failed to remove an item from the tree for topic: {topic_name}"
                            )
                        }
                    }
                    Err(_) => {
                        println!("error reading item for topic: {topic_name}");
                    }
                }
            }
        }
        Ok(())
    });

    match transaction_result {
        Ok(_) => (StatusCode::OK, Json(json!({"messageId": "tbd"}))),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "error executing produce transaction" })),
        ),
    }
}

fn increment(old: Option<&[u8]>) -> Option<Vec<u8>> {
    let number = match old {
        Some(bytes) => {
            let array: [u8; 8] = bytes.try_into().unwrap();
            let number = u64::from_be_bytes(array);
            number + 1
        }
        None => 0,
    };

    Some(number.to_be_bytes().to_vec())
}

async fn consume_handler(
    Path((topic_name, consumer_id, batch_size)): Path<(String, String, u16)>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
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

    if events.is_empty() {
        return (StatusCode::NO_CONTENT, Json(json!({ "events": [] })));
    } else {
        return (StatusCode::OK, Json(json!({ "events": events })));
    }
}

// todo - use these in program
// maybe extend From implementation of IVec and u64?
fn from_u64(input: u64) -> IVec {
    IVec::from(&input.to_be_bytes())
}

fn to_u64(input: IVec) -> Option<u64> {
    match input.to_vec().try_into() {
        Ok(i) => return Some(u64::from_be_bytes(i)),
        Err(_) => return None,
    }
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
    pub cap: u64,
    pub cap_tolerance: u64,
    pub temporary: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct Message {
    id: u64,
    data: serde_json::Value,
}

struct AppState {
    topic_db_map: HashMap<String, BobTopic>,
    consumer_state_db: Db,
}

#[derive(Debug, Deserialize, Serialize)]
struct ErrorResponse {
    message: String,
}

pub struct BobTopic {
    pub top_db: Db,
    pub topic_tree: Tree,
    pub stats_tree: Tree,
}
