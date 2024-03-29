pub mod pubsub_message;

use anyhow::{bail, Result};
use pubsub_message::{PubSubMessages, PubSubResponse};
use serde::Serialize;

pub struct PubSub {
    project_id: String,
    auth: std::sync::Arc<gcp_auth::AuthenticationManager>,
    client: reqwest::Client,
}

impl PubSub {
    pub async fn new() -> Result<Self> {
        let auth = gcp_auth::AuthenticationManager::new().await?;
        let client = reqwest::Client::builder()
            .user_agent(concat!(
                env!("CARGO_PKG_NAME"),
                "/",
                env!("CARGO_PKG_VERSION"),
            ))
            .timeout(std::time::Duration::from_secs(30))
            .https_only(true)
            .build()?;

        Ok(Self {
            project_id: auth.project_id().await?,
            auth: std::sync::Arc::new(auth),
            client,
        })
    }
    pub async fn topic(&self, topic: String) -> Result<Topic> {
        let topic = Topic {
            project_id: self.project_id.to_string(),
            auth: self.auth.clone(),
            topic,
            client: self.client.clone(),
        };
        topic.is_valid().await?;
        Ok(topic)
    }
}

#[derive(Clone)]
pub struct Topic {
    project_id: String,
    auth: std::sync::Arc<gcp_auth::AuthenticationManager>,
    topic: String,
    client: reqwest::Client,
}

impl Topic {
    pub async fn publish<S>(&self, message: &S) -> Result<String>
    where
        S: Serialize,
    {
        let response = self
            .internal_publish(&PubSubMessages::oneshot(message)?)
            .await?
            .pop()
            .unwrap();
        Ok(response)
    }

    pub async fn publish_all<S>(&self, messages: &[S]) -> Result<Vec<String>>
    where
        S: Serialize,
    {
        self.internal_publish(&messages.into()).await
    }

    pub(crate) async fn is_valid(&self) -> Result<()> {
        let url = format!(
            "https://pubsub.googleapis.com/v1/projects/{}/topics/{}",
            self.project_id, self.topic
        );
        let token = self.token().await?;
        let req = self.client.get(url).bearer_auth(&token).send().await?;
        if req.status() == 200 {
            Ok(())
        } else if req.status() == 404 {
            bail!("Topic not found")
        } else if req.status() == 403 {
            bail!("Topic query unauthorized, despite token {}", token)
        } else {
            bail!("Topic query returned status {}", req.status())
        }
    }
    async fn token(&self) -> Result<String> {
        Ok(self
            .auth
            .get_token(&["https://www.googleapis.com/auth/pubsub"])
            .await?
            .as_str()
            .trim_end_matches('.') // cursed, see https://stackoverflow.com/questions/68654502
            .to_string())
    }

    async fn internal_publish(&self, messages: &PubSubMessages) -> Result<Vec<String>> {
        let url = format!(
            "https://pubsub.googleapis.com/v1/projects/{}/topics/{}:publish",
            self.project_id, self.topic
        );
        let res = self
            .client
            .post(url)
            .bearer_auth(self.token().await?)
            .header("User-Agent", "subby_rs/0.1.0")
            .json(messages)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        let messages: PubSubResponse = serde_json::from_str(&res)?;

        Ok(messages.message_ids)
    }
}
