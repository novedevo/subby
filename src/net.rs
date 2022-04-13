use hyper::header::{AUTHORIZATION, CONTENT_TYPE, USER_AGENT};

async fn send(
    topic: &str,
    token: &str,
    body: String,
) -> std::result::Result<String, anyhow::Error> {
    let client = reqwest::Client::new();

    Ok(client
        .post(format!(
            "https://pubsub.googleapis.com/v1/{}:publish?alt=json",
            topic
        ))
        .header(USER_AGENT, "subby_rs/0.1.0")
        .header(AUTHORIZATION, format!("Bearer {}", token))
        .header(CONTENT_TYPE, "application/json")
        //todo: add content length and content type, if it fails
        //todo: make this grpc
        .body(body)
        .send()
        .await?
        .text()
        .await?)
}
