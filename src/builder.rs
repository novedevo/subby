use crate::{AuthProvider, Authenticator, PubSub};
use anyhow::{bail, Result};

type SAKey = yup_oauth2::ServiceAccountKey;

#[derive(Default, Debug)]
pub struct UnbuiltPubSub {
    project_id: Option<String>,
    gce: bool,
    sa_key: Option<SAKey>,
}

impl UnbuiltPubSub {
    pub async fn build(mut self) -> Result<PubSub> {
        let client = reqwest::Client::new();
        let auth_provider = if let Some(sa_key) = self.auto_get_sa_key().await? {
            self.sa_key = Some(sa_key.clone());
            AuthProvider::SA(Self::build_auth(sa_key).await?)
        } else if crate::net::is_on_gce(&client).await {
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
        yup_oauth2::ServiceAccountAuthenticator::builder(sa_key)
            .build()
            .await
    }
    async fn get_project_id(&mut self) -> Result<String> {
        Ok(if let Some(proj_id) = self.project_id.take() {
            proj_id
        } else if let Ok(id) = std::env::var("GCLOUD_PROJECT_ID") {
            id
        } else if self.gce {
            crate::net::gce_project_id(&reqwest::Client::new()).await?
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
        let key = std::fs::read_to_string(keypath)?;
        let key = yup_oauth2::parse_service_account_key(key)?;
        Ok(key)
    }
}
