pub async fn publish(
    client: &reqwest::Client,
    topic: &str,
    project: &str,
    token: &str,
    body: String,
) -> reqwest::Result<String> {
    client
        .post(format!(
            "https://pubsub.googleapis.com/v1/projects/{}/topics/{}:publish",
            project, topic
        ))
        .header("User-Agent", "subby_rs/0.1.0")
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/json")
        //todo: add content length and content type, if it fails
        //todo: make this grpc
        .body(body)
        .send()
        .await?
        .text()
        .await
}

pub async fn is_on_gce(client: &reqwest::Client) -> bool {
    gce_project_id(client).await.is_ok()
}

pub async fn gce_token(client: &reqwest::Client) -> reqwest::Result<String> {
    metadata(client, "instance/service_accounts/default/token").await
}
pub async fn gce_project_id(client: &reqwest::Client) -> reqwest::Result<String> {
    metadata(client, "project/project_id").await
}

async fn metadata(client: &reqwest::Client, path: &str) -> reqwest::Result<String> {
    client
        .get(format!(
            "http://metadata.google.internal/computeMetadata/v1/{}",
            path
        ))
        .header("User-Agent", "subby_rs/0.1.0")
        .header("Metadata-Flavor", "Google")
        .send()
        .await?
        .text()
        .await
}
