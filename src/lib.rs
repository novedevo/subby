mod net;

use anyhow::{bail, Result};
use hyper::client::connect::HttpConnector;
use hyper_rustls::HttpsConnector;
use oauth2::{storage::TokenInfo, AccessToken};
use yup_oauth2 as oauth2;

#[derive(Default, Debug)]
pub struct UnbuiltPubSub {
    project_id: Option<String>,
    gce: bool,
    sa_key: Option<SAKey>,
}

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

type SAKey = oauth2::ServiceAccountKey;
type Authenticator = oauth2::authenticator::Authenticator<HttpsConnector<HttpConnector>>;

impl UnbuiltPubSub {
    pub async fn build(mut self) -> Result<PubSub> {
        let client = reqwest::Client::new();
        let auth_provider = if let Some(sa_key) = self.auto_get_sa_key().await? {
            self.sa_key = Some(sa_key.clone());
            AuthProvider::SA(Self::build_auth(sa_key).await?)
        } else if net::is_on_gce(&client).await {
            self.gce = true;
            AuthProvider::Gce(client)
        } else {
            bail!("Failed to discover authentication credentials from environment")
        };
        let project_id = self.get_project_id().await?;
        Ok(PubSub {
            project_id,
            auth_provider,
        })
    }

    async fn build_auth(sa_key: SAKey) -> std::result::Result<Authenticator, std::io::Error> {
        oauth2::ServiceAccountAuthenticator::builder(sa_key)
            .build()
            .await
    }
    async fn get_project_id(&mut self) -> Result<String> {
        Ok(if let Some(proj_id) = self.project_id.take() {
            proj_id
        } else if let Ok(id) = std::env::var("GCLOUD_PROJECT_ID") {
            id
        } else if self.gce {
            net::gce_project_id(&reqwest::Client::new()).await?
        } else if let Some(sa_key) = self.sa_key.as_ref().and_then(|sa| sa.project_id.as_ref()) {
            sa_key.clone()
        } else {
            bail!("No project ID found (is your keyfile missing data?)")
        })
    }
    async fn auto_get_sa_key(&mut self) -> Result<Option<SAKey>> {
        if let Some(sa_key) = self.sa_key.take() {
            Ok(Some(sa_key))
        } else if let Ok(keypath) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            Ok(Some(Self::get_sa_key(&keypath).await?))
        } else {
            Ok(None)
        }
    }
    pub async fn set_sa_key(&mut self, keypath: &str) -> Result<()> {
        self.sa_key = Some(Self::get_sa_key(keypath).await?);
        Ok(())
    }
    async fn get_sa_key(keypath: &str) -> Result<SAKey> {
        let key = tokio::fs::read_to_string(keypath).await?;
        let key = oauth2::parse_service_account_key(key)?;
        Ok(key)
    }
}

#[allow(clippy::large_enum_variant)]
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
