use bob_ka::BobConfig;
use std::error::Error;
use std::fs;
use toml::de::from_str;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // load the old config up
    // todo - make config file a param to bin
    let config: BobConfig = match fs::read_to_string("config.toml") {
        Ok(content) => from_str(&content).expect("unable to parse config into struct"),
        Err(e) => panic!("cannot read config.toml!!: {e}"),
    };

    let result = bob_ka::start_web_server(config).await;
    match result {
        Ok(_) => return Ok(()),
        Err(e) => return Err(e),
    }
}
