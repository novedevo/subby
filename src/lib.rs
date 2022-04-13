pub mod builder;
mod net;

use anyhow::Result;
use hyper::client::connect::HttpConnector;
use hyper_rustls::HttpsConnector;
use oauth2::{storage::TokenInfo, AccessToken};
use yup_oauth2 as oauth2;

pub struct PubSub {
    project_id: String,
    auth_provider: AuthProvider,
}

impl PubSub {
    pub async fn topic(self, topic: String) -> Result<Topic> {
        Ok(Topic {
            project_id: self.project_id,
            token: self.auth_provider.token().await?,
            auth_provider: self.auth_provider,
            topic,
            client: reqwest::Client::new(),
        })
    }
}

pub struct Topic {
    project_id: String,
    auth_provider: AuthProvider,
    token: AccessToken,
    topic: String,
    client: reqwest::Client,
}

impl Topic {
    pub async fn publish<S>(&mut self, message: S) -> Result<String>
    where
        S: serde::Serialize,
    {
        if self.token.is_expired() {
            self.token = self.auth_provider.token().await?;
        }
        let res = net::publish(
            &self.client,
            &self.topic,
            &self.project_id,
            self.token.as_str(),
            serde_json::to_string(&message)?,
        )
        .await?;
        Ok(res)
    }
}

type Authenticator = oauth2::authenticator::Authenticator<HttpsConnector<HttpConnector>>;

enum AuthProvider {
    Gce(reqwest::Client),
    SA(Authenticator),
}

impl AuthProvider {
    async fn token(&self) -> Result<AccessToken> {
        let token: AccessToken = match self {
            Self::Gce(client) => {
                serde_json::from_str::<GceResponse>(&net::gce_token(client).await?)?.into()
            }
            Self::SA(auth) => {
                auth.token(&["https://www.googleapis.com/auth/pubsub"])
                    .await?
            }
        };
        Ok(token)
    }
}

#[derive(serde::Deserialize)]
struct GceResponse {
    access_token: String,
    expires_in: i64,
}

impl From<GceResponse> for AccessToken {
    fn from(resp: GceResponse) -> Self {
        TokenInfo {
            access_token: resp.access_token,
            expires_at: Some(
                time::OffsetDateTime::now_utc() + time::Duration::seconds(resp.expires_in),
            ),
            id_token: None,
            refresh_token: None,
        }
        .into()
    }
}
