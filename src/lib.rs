mod net;

use anyhow::Result;

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
    pub fn topic(&self, topic: String) -> Result<Topic<'_>> {
        Ok(Topic {
            project_id: &self.project_id,
            auth: &self.auth,
            topic,
            client: &self.client,
        })
    }
}

pub struct Topic<'a> {
    project_id: &'a str,
    auth: &'a gcp_auth::AuthenticationManager,
    topic: String,
    client: &'a reqwest::Client,
}

impl Topic<'_> {
    pub async fn publish<S>(&mut self, message: &S) -> Result<String>
    where
        S: serde::Serialize,
    {
        let res = net::publish(
            self.client,
            &self.topic,
            self.project_id,
            self.auth
                .get_token(&["https://www.googleapis.com/auth/pubsub"])
                .await?
                .as_str(),
            message,
        )
        .await?;
        Ok(res)
    }
}
