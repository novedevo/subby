use anyhow::{bail, Result};

pub struct PubSub {
    project_id: String,
    auth: gcp_auth::AuthenticationManager,
    client: reqwest::Client,
}

impl PubSub {
    pub async fn new() -> Result<Self> {
        let auth = gcp_auth::AuthenticationManager::new().await?;
        Ok(Self {
            project_id: auth.project_id().await?,
            auth,
            client: reqwest::Client::new(),
        })
    }
    pub async fn topic(&self, topic: String) -> Result<Topic<'_>> {
        let topic = Topic {
            project_id: &self.project_id,
            auth: &self.auth,
            topic,
            client: &self.client,
        };
        if !topic.is_valid().await {
            bail!("Invalid topic name");
        }
        Ok(topic)
    }
}

pub struct Topic<'a> {
    project_id: &'a str,
    auth: &'a gcp_auth::AuthenticationManager,
    topic: String,
    client: &'a reqwest::Client,
}

impl Topic<'_> {
    pub async fn publish<S>(&self, message: &S) -> Result<String>
    where
        S: serde::Serialize,
    {
        let url = format!(
            "https://pubsub.googleapis.com/v1/projects/{}/topics/{}:publish",
            self.project_id, self.topic
        );
        let token = self
            .auth
            .get_token(&["https://www.googleapis.com/auth/pubsub"])
            .await?;
        let res = self
            .client
            .post(url)
            .header("User-Agent", "subby_rs/0.1.0")
            .header("Authorization", format!("Bearer {}", token.as_str()))
            .json(message)
            .send()
            .await?
            .text()
            .await?;

        Ok(res)
    }
    pub(crate) async fn is_valid(&self) -> bool {
        let url = format!(
            "https://pubsub.googleapis.com/v1/projects/{}/topics/{}",
            self.project_id, self.topic
        );
        let req = self.client.get(url).send().await;
        if let Ok(res) = req {
            res.status() == 200
        } else {
            false
        }
    }
}
