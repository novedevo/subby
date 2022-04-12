use anyhow::{bail, Result};
use gcemeta::Client;
use google_pubsub1 as pubsub1;
use hyper::{body::Body, client::connect::HttpConnector};
use pubsub1::{hyper, hyper_rustls, oauth2, Pubsub};

#[derive(Default, Debug)]
pub struct UnbuiltPubSub {
    project_id: Option<String>,
    gce: Option<Gce>,
    sa_key: Option<SAKey>,
}

pub struct PubSub {
    project_id: String,
    auth_provider: AuthProvider,
}

type Gce = Client<HttpConnector, Body>;
type SAKey = oauth2::ServiceAccountKey;

impl UnbuiltPubSub {
    pub async fn build(self) -> Result<PubSub> {
        let auth = if self.gce.is_none() && self.sa_key.is_none() {
            if self.auto_get_sa_key().await.is_err() && self.auto_get_gce().await.is_err() {
                bail!("Unable to autodiscover authentication variables.");
            } else if let Some(sa_key) = self.sa_key {
                AuthProvider::SAKey(sa_key)
            } else if let Some(gce) = self.gce {
                AuthProvider::Gce(gce)
            } else {
                unreachable!()
            }
        } else if let Some(sa_key) = self.sa_key {
            AuthProvider::SAKey(sa_key)
        } else if let Some(gce) = self.gce {
            AuthProvider::Gce(gce)
        } else {
            unreachable!()
        };
        return 1;
    }

    async fn auto_get_gce(&mut self) -> Result<()> {
        let gce = Client::new();
        if gce.on_gce().await? {
            self.gce = Some(gce);
            Ok(())
        } else {
            bail!("Not on GCE")
        }
    }
    async fn get_project_id(&self) -> Result<String> {
        if let Some(project_id) = &self.project_id {
            Ok(project_id.clone())
        } else if let Ok(id) = std::env::var("GCLOUD_PROJECT_ID") {
            Ok(id)
        } else if let Some(gce) = &self.gce {
            Ok(gce.project_id().await?)
        } else if let Some(SAKey {
            project_id: Some(project_id),
            ..
        }) = &self.sa_key
        {
            Ok(project_id.clone())
        } else {
            bail!("Failed to discover project ID")
        }
    }
    async fn auto_get_sa_key(&mut self) -> Result<()> {
        if let Ok(keypath) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            self.set_sa_key(&keypath).await?;
            Ok(())
        } else {
            bail!("No env variable found")
        }
    }
    pub async fn set_sa_key(&mut self, keypath: &str) -> Result<()> {
        let key = tokio::fs::read_to_string(keypath).await?;
        let key = oauth2::parse_service_account_key(key)?;
        self.sa_key = Some(key);
        Ok(())
    }
    async fn get_auth(&self) -> Result<String> {
        // let auth = oauth2::ApplicationSecret{}
        if let Ok(keypath) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            let key = tokio::fs::read_to_string(keypath).await?;
            let key = oauth2::parse_service_account_key(key)?;
            Ok(key.private_key)
        } else if let Some(gce) = &self.gce {
            Ok(gce.token(None).await?)
        } else {
            bail!("failed to get authentication credentials")
        }
    }
}

enum AuthProvider {
    Gce(Gce),
    SAKey(SAKey),
}
