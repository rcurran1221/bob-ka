use axum::{routing::get, Router};
use std::net::SocketAddr;

async fn hello() -> &'static str {
    "Hello, world!"
}

#[tokio::main]
async fn main() {
    // Build the application with a route
    let app = Router::new().route("/", get(hello));

    // Define the address to bind the server
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    println!("Listening on http://{}", addr);

    // Run the server
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
