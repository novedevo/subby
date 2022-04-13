pub async fn publish<S>(
    client: &reqwest::Client,
    topic: &str,
    project: &str,
    token: &str,
    body: &S,
) -> reqwest::Result<String>
where
    S: serde::Serialize,
{
    client
        .post(format!(
            "https://pubsub.googleapis.com/v1/projects/{}/topics/{}:publish",
            project, topic
        ))
        .header("User-Agent", "subby_rs/0.1.0")
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/json")
        //todo: add content length, if it fails
        //todo: make this grpc
        .json(body)
        .send()
        .await?
        .text()
        .await
}
