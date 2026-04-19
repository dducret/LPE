use anyhow::{anyhow, bail, Result};
use argon2::{
    password_hash::{PasswordHash, PasswordVerifier},
    Argon2,
};
use axum::http::HeaderMap;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lpe_storage::{AccountLogin, AuthenticatedAccount, Storage};
use std::{future::Future, pin::Pin};

pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountPrincipal {
    pub tenant_id: String,
    pub account_id: uuid::Uuid,
    pub email: String,
    pub display_name: String,
}

pub trait AccountAuthStore: Clone + Send + Sync + 'static {
    fn fetch_account_session<'a>(
        &'a self,
        token: &'a str,
    ) -> StoreFuture<'a, Option<AuthenticatedAccount>>;
    fn fetch_account_login<'a>(&'a self, email: &'a str) -> StoreFuture<'a, Option<AccountLogin>>;
}

impl AccountAuthStore for Storage {
    fn fetch_account_session<'a>(
        &'a self,
        token: &'a str,
    ) -> StoreFuture<'a, Option<AuthenticatedAccount>> {
        Box::pin(async move { self.fetch_account_session(token).await })
    }

    fn fetch_account_login<'a>(&'a self, email: &'a str) -> StoreFuture<'a, Option<AccountLogin>> {
        Box::pin(async move { self.fetch_account_login(email).await })
    }
}

pub async fn authenticate_account<S: AccountAuthStore>(
    store: &S,
    hinted_user: Option<&str>,
    headers: &HeaderMap,
) -> Result<AccountPrincipal> {
    if let Some(token) = bearer_token(headers) {
        if let Some(account) = store.fetch_account_session(&token).await? {
            return Ok(AccountPrincipal {
                tenant_id: account.tenant_id,
                account_id: account.account_id,
                email: account.email,
                display_name: account.display_name,
            });
        }
    }

    if let Some((username, password)) = basic_credentials(headers)? {
        let login = store
            .fetch_account_login(&normalize_login_name(&username, hinted_user))
            .await?
            .ok_or_else(|| anyhow!("invalid credentials"))?;
        if login.status != "active" || !verify_password(&login.password_hash, &password) {
            bail!("invalid credentials");
        }
        return Ok(AccountPrincipal {
            tenant_id: login.tenant_id,
            account_id: login.account_id,
            email: login.email,
            display_name: login.display_name,
        });
    }

    bail!("missing account authentication");
}

pub fn normalize_login_name(username: &str, hinted_user: Option<&str>) -> String {
    if username.contains('@') {
        username.trim().to_lowercase()
    } else {
        hinted_user.unwrap_or(username).trim().to_lowercase()
    }
}

pub fn verify_password(password_hash: &str, password: &str) -> bool {
    PasswordHash::new(password_hash)
        .ok()
        .and_then(|parsed| {
            Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .ok()
        })
        .is_some()
}

pub fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

pub fn basic_credentials(headers: &HeaderMap) -> Result<Option<(String, String)>> {
    let Some(value) = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(None);
    };
    let Some(encoded) = value.strip_prefix("Basic ") else {
        return Ok(None);
    };
    let decoded = BASE64.decode(encoded.trim())?;
    let decoded = String::from_utf8(decoded)?;
    let (username, password) = decoded
        .split_once(':')
        .ok_or_else(|| anyhow!("invalid basic authorization header"))?;
    Ok(Some((username.to_string(), password.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use argon2::password_hash::{rand_core::OsRng, PasswordHasher, SaltString};
    use axum::http::{header::AUTHORIZATION, HeaderValue};
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    #[derive(Clone, Default)]
    struct FakeStore {
        session: Arc<Mutex<Option<AuthenticatedAccount>>>,
        login: Arc<Mutex<Option<AccountLogin>>>,
    }

    impl AccountAuthStore for FakeStore {
        fn fetch_account_session<'a>(
            &'a self,
            _token: &'a str,
        ) -> StoreFuture<'a, Option<AuthenticatedAccount>> {
            Box::pin(async move { Ok(self.session.lock().unwrap().clone()) })
        }

        fn fetch_account_login<'a>(
            &'a self,
            _email: &'a str,
        ) -> StoreFuture<'a, Option<AccountLogin>> {
            Box::pin(async move { Ok(self.login.lock().unwrap().clone()) })
        }
    }

    fn password_hash(password: &str) -> String {
        Argon2::default()
            .hash_password(password.as_bytes(), &SaltString::generate(&mut OsRng))
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn bearer_session_preserves_tenant_id() {
        let store = FakeStore {
            session: Arc::new(Mutex::new(Some(AuthenticatedAccount {
                tenant_id: "tenant-a".to_string(),
                account_id: Uuid::nil(),
                email: "alice@example.test".to_string(),
                display_name: "Alice".to_string(),
                expires_at: "2099-01-01T00:00:00Z".to_string(),
            }))),
            login: Arc::default(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer token"));

        let principal = authenticate_account(&store, None, &headers).await.unwrap();

        assert_eq!(principal.tenant_id, "tenant-a");
    }

    #[tokio::test]
    async fn basic_auth_preserves_tenant_id() {
        let encoded = BASE64.encode("alice@example.test:secret");
        let store = FakeStore {
            session: Arc::default(),
            login: Arc::new(Mutex::new(Some(AccountLogin {
                tenant_id: "tenant-b".to_string(),
                account_id: Uuid::nil(),
                email: "alice@example.test".to_string(),
                password_hash: password_hash("secret"),
                status: "active".to_string(),
                display_name: "Alice".to_string(),
            }))),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {encoded}")).unwrap(),
        );

        let principal = authenticate_account(&store, None, &headers).await.unwrap();

        assert_eq!(principal.tenant_id, "tenant-b");
    }

    #[tokio::test]
    async fn hinted_user_does_not_override_login_tenant() {
        let encoded = BASE64.encode("alice:secret");
        let store = FakeStore {
            session: Arc::default(),
            login: Arc::new(Mutex::new(Some(AccountLogin {
                tenant_id: "tenant-c".to_string(),
                account_id: Uuid::nil(),
                email: "alice@example.test".to_string(),
                password_hash: password_hash("secret"),
                status: "active".to_string(),
                display_name: "Alice".to_string(),
            }))),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {encoded}")).unwrap(),
        );

        let principal = authenticate_account(&store, Some("alice@example.test"), &headers)
            .await
            .unwrap();

        assert_eq!(principal.email, "alice@example.test");
        assert_eq!(principal.tenant_id, "tenant-c");
    }
}
