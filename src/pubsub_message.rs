use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug)]
pub struct PubSubMessages {
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

impl<S> From<&[S]> for PubSubMessages
where
    S: Serialize,
{
    fn from(messages: &[S]) -> Self {
        Self {
            messages: messages.iter().map(|s| (s,).into()).collect(),
        }
    }
}

//We can't implement it for all S because it would conflict with the blanket From impl, waiting on specialization to fix this
impl<S> From<(S,)> for PubSubMessage
where
    S: Serialize,
{
    fn from(message: (S,)) -> Self {
        let json = serde_json::to_vec(&message.0).expect("serialization to work");
        let data = base64::encode_config(json, base64::URL_SAFE);
        Self { data }
    }
}

#[derive(Serialize, Debug)]
pub struct PubSubMessage {
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PubSubResponse {
    pub message_ids: Vec<String>,
}
