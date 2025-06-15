use axum::{Router, routing::get};
use serde::Deserialize;
use std::fs;
use std::net::SocketAddr;
use toml::de::from_str;
use std::error::Error;

#[tokio::main]
async fn main()  -> Result<(), Box<dyn Error>>{
    // load the old config up
    let config : Config = match fs::read_to_string("config.toml") {
        Ok(content) => from_str(&content).expect("unable to parse config into struct"),
        Err(_) => panic!("cannot read config.toml!!"),
    };

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
struct Config {
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
    mmap: bool,
    compression: bool,
}
