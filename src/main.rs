use bob_ka::BobConfig;
use std::error::Error;
use std::{env, fs};
use toml::de::from_str;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let config_location = &args[1];
    let dbs_location = &args[2];
    println!("reading config from: {config_location}");
    let config: BobConfig = match fs::read_to_string(config_location) {
        Ok(content) => from_str(&content).expect("unable to parse config into struct"),
        Err(e) => panic!("cannot read config.toml!!: {e}"),
    };

    let result = bob_ka::start_web_server(config).await;
    match result {
        Ok(_) => return Ok(()),
        Err(e) => return Err(e),
    }
}
