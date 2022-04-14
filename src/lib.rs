use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

pub struct PubSub {
    project_id: String,
    auth: gcp_auth::AuthenticationManager,
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
            auth,
            client,
        })
    }
    pub async fn topic(&self, topic: String) -> Result<Topic<'_>> {
        let topic = Topic {
            project_id: &self.project_id,
            auth: &self.auth,
            topic,
            client: &self.client,
        };
        topic.is_valid().await?;
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
        let req = self
            .client
            .get(url)
            .bearer_auth(self.token().await?)
            .send()
            .await?;
        if req.status() == 200 {
            Ok(())
        } else if req.status() == 404 {
            bail!("Topic not found")
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
            .text()
            .await?;

        let messages: PubSubResponse = serde_json::from_str(&res)?;

        Ok(messages.message_ids)
    }
}

#[derive(Serialize, Debug)]
struct PubSubMessages {
    messages: Vec<PubSubMessage>,
}

impl PubSubMessages {
    pub(crate) fn oneshot<S>(message: &S) -> Result<Self>
    where
        S: Serialize,
    {
        Ok(Self {
            messages: vec![PubSubMessage::new(message)?],
        })
    }
}

#[allow(clippy::from_over_into)] //we can't convert PubSubMessages into anything serializable, lol
impl<S> Into<PubSubMessages> for &[S]
where
    S: Serialize,
{
    fn into(self) -> PubSubMessages {
        let messages = self.iter().map(|s| (s,).into()).collect();
        PubSubMessages { messages }
    }
}

#[allow(clippy::from_over_into)]
//We can't implement it for all S because of weird generic foreign trait impls that I don't understand
impl<S> Into<PubSubMessage> for (S,)
where
    S: Serialize,
{
    fn into(self) -> PubSubMessage {
        let json = serde_json::to_vec(&self.0).expect("serde to work");
        let bytes = base64::encode_config(json, base64::URL_SAFE);
        PubSubMessage { data: bytes }
    }
}

#[derive(Serialize, Debug)]
struct PubSubMessage {
    data: String,
}

impl PubSubMessage {
    pub(crate) fn new<S>(message: &S) -> Result<Self>
    where
        S: Serialize,
    {
        let json = serde_json::to_vec(message)?;
        let bytes = base64::encode_config(json, base64::URL_SAFE);
        Ok(Self { data: bytes })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PubSubResponse {
    message_ids: Vec<String>,
}
