mod auth;
mod oauth;
mod store;

pub use crate::auth::{
    authenticate_account, authenticate_bearer_access_token, authenticate_plain_credentials,
    normalize_login_name, verify_password,
};
pub use crate::oauth::{
    basic_credentials, bearer_token, issue_oauth_access_token, normalize_scope,
    oauth_signing_secret, unix_time, AccountPrincipal, DEFAULT_OAUTH_ACCESS_SCOPE,
    DEFAULT_OAUTH_ACCESS_TOKEN_SECONDS,
};
pub use crate::store::{AccountAuthStore, StoreFuture};

#[cfg(test)]
mod tests {
    use super::*;
    use argon2::password_hash::{rand_core::OsRng, PasswordHasher, SaltString};
    use axum::http::{header::AUTHORIZATION, HeaderMap, HeaderValue};
    use base64::Engine;
    use lpe_storage::{
        AccountLogin, AuditEntryInput, AuthenticatedAccount, StoredAccountAppPassword,
    };
    use std::sync::{Arc, Mutex, MutexGuard};
    use uuid::Uuid;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn env_lock() -> MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[derive(Clone, Default)]
    struct FakeStore {
        session: Arc<Mutex<Option<AuthenticatedAccount>>>,
        login: Arc<Mutex<Option<AccountLogin>>>,
        app_passwords: Arc<Mutex<Vec<StoredAccountAppPassword>>>,
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

        fn fetch_active_account_app_passwords<'a>(
            &'a self,
            _email: &'a str,
        ) -> StoreFuture<'a, Vec<StoredAccountAppPassword>> {
            Box::pin(async move { Ok(self.app_passwords.lock().unwrap().clone()) })
        }

        fn touch_account_app_password<'a>(
            &'a self,
            _email: &'a str,
            _app_password_id: Uuid,
        ) -> StoreFuture<'a, ()> {
            Box::pin(async move { Ok(()) })
        }

        fn append_audit_event<'a>(
            &'a self,
            _tenant_id: &'a str,
            _entry: AuditEntryInput,
        ) -> StoreFuture<'a, ()> {
            Box::pin(async move { Ok(()) })
        }
    }

    fn password_hash(password: &str) -> String {
        argon2::Argon2::default()
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
            app_passwords: Arc::default(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer token"));

        let principal = authenticate_account(&store, None, &headers, "dav")
            .await
            .unwrap();

        assert_eq!(principal.tenant_id, "tenant-a");
    }

    #[tokio::test]
    async fn basic_auth_preserves_tenant_id() {
        let encoded = base64::engine::general_purpose::STANDARD.encode("alice@example.test:secret");
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
            app_passwords: Arc::default(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {encoded}")).unwrap(),
        );

        let principal = authenticate_account(&store, None, &headers, "dav")
            .await
            .unwrap();

        assert_eq!(principal.tenant_id, "tenant-b");
    }

    #[tokio::test]
    async fn hinted_user_does_not_override_login_tenant() {
        let encoded = base64::engine::general_purpose::STANDARD.encode("alice:secret");
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
            app_passwords: Arc::default(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {encoded}")).unwrap(),
        );

        let principal = authenticate_account(&store, Some("alice@example.test"), &headers, "dav")
            .await
            .unwrap();

        assert_eq!(principal.email, "alice@example.test");
        assert_eq!(principal.tenant_id, "tenant-c");
    }

    #[tokio::test]
    async fn app_password_is_accepted_for_basic_auth() {
        let encoded =
            base64::engine::general_purpose::STANDARD.encode("alice@example.test:device-secret");
        let store = FakeStore {
            session: Arc::default(),
            login: Arc::new(Mutex::new(Some(AccountLogin {
                tenant_id: "tenant-d".to_string(),
                account_id: Uuid::nil(),
                email: "alice@example.test".to_string(),
                password_hash: password_hash("primary-secret"),
                status: "active".to_string(),
                display_name: "Alice".to_string(),
            }))),
            app_passwords: Arc::new(Mutex::new(vec![StoredAccountAppPassword {
                id: Uuid::new_v4(),
                password_hash: password_hash("device-secret"),
            }])),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {encoded}")).unwrap(),
        );

        let principal = authenticate_account(&store, None, &headers, "imap")
            .await
            .unwrap();

        assert_eq!(principal.tenant_id, "tenant-d");
    }

    #[tokio::test]
    #[ignore = "env-sensitive"]
    async fn oauth_access_token_is_accepted_for_bearer_auth() {
        let _guard = env_lock();
        std::env::set_var(
            "LPE_MAIL_OAUTH_SIGNING_SECRET",
            "0123456789abcdef0123456789abcdef",
        );
        let login = AccountLogin {
            tenant_id: "tenant-e".to_string(),
            account_id: Uuid::new_v4(),
            email: "alice@example.test".to_string(),
            password_hash: password_hash("secret"),
            status: "active".to_string(),
            display_name: "Alice".to_string(),
        };
        let principal = AccountPrincipal {
            tenant_id: login.tenant_id.clone(),
            account_id: login.account_id,
            email: login.email.clone(),
            display_name: login.display_name.clone(),
        };
        let token = issue_oauth_access_token(&principal, "dav activesync", 600).unwrap();
        let store = FakeStore {
            session: Arc::default(),
            login: Arc::new(Mutex::new(Some(login))),
            app_passwords: Arc::default(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );

        let authenticated = authenticate_account(&store, None, &headers, "dav")
            .await
            .unwrap();

        assert_eq!(authenticated.email, "alice@example.test");
        std::env::remove_var("LPE_MAIL_OAUTH_SIGNING_SECRET");
    }

    #[tokio::test]
    #[ignore = "env-sensitive"]
    async fn oauth_access_token_rejects_surface_outside_scope() {
        let _guard = env_lock();
        std::env::set_var(
            "LPE_MAIL_OAUTH_SIGNING_SECRET",
            "0123456789abcdef0123456789abcdef",
        );
        let login = AccountLogin {
            tenant_id: "tenant-f".to_string(),
            account_id: Uuid::new_v4(),
            email: "alice@example.test".to_string(),
            password_hash: password_hash("secret"),
            status: "active".to_string(),
            display_name: "Alice".to_string(),
        };
        let principal = AccountPrincipal {
            tenant_id: login.tenant_id.clone(),
            account_id: login.account_id,
            email: login.email.clone(),
            display_name: login.display_name.clone(),
        };
        let token = issue_oauth_access_token(&principal, "dav", 600).unwrap();
        let store = FakeStore {
            session: Arc::default(),
            login: Arc::new(Mutex::new(Some(login))),
            app_passwords: Arc::default(),
        };

        let error = authenticate_bearer_access_token(&store, None, &token, "imap")
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("oauth access token is not valid for this surface"));
        std::env::remove_var("LPE_MAIL_OAUTH_SIGNING_SECRET");
    }

    #[test]
    fn normalize_scope_accepts_smtp_surface() {
        let scope = normalize_scope("smtp mail").unwrap();
        assert_eq!(scope, "mail smtp");
    }

    #[test]
    fn normalize_scope_accepts_ews_surface() {
        let scope = normalize_scope("ews mail").unwrap();
        assert_eq!(scope, "ews mail");
    }
}
