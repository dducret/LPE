mod auth;
mod parse;
mod service;
mod store;

pub use crate::service::{serve, ManageSieveServer};
pub use crate::store::{ManageSieveStore, StoreFuture};

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{anyhow, Result};
    use argon2::{
        password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
        Argon2,
    };
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
    use lpe_mail_auth::{issue_oauth_access_token, AccountAuthStore};
    use lpe_storage::{
        AccountLogin, AuditEntryInput, SieveScriptDocument, SieveScriptSummary,
    };
    use std::sync::{Arc, Mutex};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    use uuid::Uuid;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Clone)]
    struct FakeStore {
        session: Option<lpe_storage::AuthenticatedAccount>,
        login: AccountLogin,
        scripts: Arc<Mutex<Vec<SieveScriptDocument>>>,
        active: Arc<Mutex<Option<String>>>,
    }

    impl FakeStore {
        fn new() -> Self {
            Self {
                session: None,
                login: AccountLogin {
                    tenant_id: "tenant-a".to_string(),
                    account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
                    email: "alice@example.test".to_string(),
                    password_hash: Argon2::default()
                        .hash_password(b"secret", &SaltString::generate(&mut OsRng))
                        .unwrap()
                        .to_string(),
                    status: "active".to_string(),
                    display_name: "Alice".to_string(),
                },
                scripts: Arc::new(Mutex::new(Vec::new())),
                active: Arc::new(Mutex::new(None)),
            }
        }
    }

    impl AccountAuthStore for FakeStore {
        fn fetch_account_session<'a>(
            &'a self,
            token: &'a str,
        ) -> StoreFuture<'a, Option<lpe_storage::AuthenticatedAccount>> {
            let session = if token == "session-token" {
                self.session.clone()
            } else {
                None
            };
            Box::pin(async move { Ok(session) })
        }

        fn fetch_account_login<'a>(
            &'a self,
            email: &'a str,
        ) -> StoreFuture<'a, Option<AccountLogin>> {
            let login = if email.eq_ignore_ascii_case(&self.login.email) {
                Some(self.login.clone())
            } else {
                None
            };
            Box::pin(async move { Ok(login) })
        }

        fn fetch_active_account_app_passwords<'a>(
            &'a self,
            _email: &'a str,
        ) -> StoreFuture<'a, Vec<lpe_storage::StoredAccountAppPassword>> {
            Box::pin(async move { Ok(Vec::new()) })
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

    impl ManageSieveStore for FakeStore {
        fn list_sieve_scripts<'a>(
            &'a self,
            _account_id: Uuid,
        ) -> StoreFuture<'a, Vec<SieveScriptSummary>> {
            let scripts = self.scripts.lock().unwrap().clone();
            Box::pin(async move {
                Ok(scripts
                    .into_iter()
                    .map(|script| SieveScriptSummary {
                        name: script.name,
                        is_active: script.is_active,
                        size_octets: script.content.len() as u64,
                        updated_at: script.updated_at,
                    })
                    .collect())
            })
        }

        fn get_sieve_script<'a>(
            &'a self,
            _account_id: Uuid,
            name: &'a str,
        ) -> StoreFuture<'a, Option<SieveScriptDocument>> {
            let scripts = self.scripts.lock().unwrap().clone();
            let name = name.to_string();
            Box::pin(async move {
                Ok(scripts
                    .into_iter()
                    .find(|script| script.name.eq_ignore_ascii_case(&name)))
            })
        }

        fn put_sieve_script<'a>(
            &'a self,
            _account_id: Uuid,
            name: &'a str,
            content: &'a str,
            activate: bool,
            _audit: AuditEntryInput,
        ) -> StoreFuture<'a, SieveScriptDocument> {
            let scripts = self.scripts.clone();
            let name = name.to_string();
            let content = content.to_string();
            Box::pin(async move {
                let mut guard = scripts.lock().unwrap();
                guard.retain(|script| !script.name.eq_ignore_ascii_case(&name));
                guard.push(SieveScriptDocument {
                    name: name.clone(),
                    content,
                    is_active: activate,
                    updated_at: "2026-04-19T00:00:00Z".to_string(),
                });
                Ok(guard.last().unwrap().clone())
            })
        }

        fn delete_sieve_script<'a>(
            &'a self,
            _account_id: Uuid,
            name: &'a str,
            _audit: AuditEntryInput,
        ) -> StoreFuture<'a, ()> {
            let scripts = self.scripts.clone();
            let name = name.to_string();
            Box::pin(async move {
                scripts
                    .lock()
                    .unwrap()
                    .retain(|script| !script.name.eq_ignore_ascii_case(&name));
                Ok(())
            })
        }

        fn rename_sieve_script<'a>(
            &'a self,
            _account_id: Uuid,
            old_name: &'a str,
            new_name: &'a str,
            _audit: AuditEntryInput,
        ) -> StoreFuture<'a, SieveScriptSummary> {
            let scripts = self.scripts.clone();
            let old_name = old_name.to_string();
            let new_name = new_name.to_string();
            Box::pin(async move {
                let mut guard = scripts.lock().unwrap();
                let script = guard
                    .iter_mut()
                    .find(|script| script.name.eq_ignore_ascii_case(&old_name))
                    .ok_or_else(|| anyhow!("missing script"))?;
                script.name = new_name.clone();
                Ok(SieveScriptSummary {
                    name: script.name.clone(),
                    is_active: script.is_active,
                    size_octets: script.content.len() as u64,
                    updated_at: script.updated_at.clone(),
                })
            })
        }

        fn set_active_sieve_script<'a>(
            &'a self,
            _account_id: Uuid,
            name: Option<&'a str>,
            _audit: AuditEntryInput,
        ) -> StoreFuture<'a, Option<String>> {
            let active = self.active.clone();
            let scripts = self.scripts.clone();
            let selected = name.map(ToString::to_string);
            Box::pin(async move {
                *active.lock().unwrap() = selected.clone();
                for script in scripts.lock().unwrap().iter_mut() {
                    script.is_active = selected
                        .as_ref()
                        .map(|value| script.name.eq_ignore_ascii_case(value))
                        .unwrap_or(false);
                }
                Ok(selected)
            })
        }
    }

    #[test]
    fn parses_putscript_request_line_with_literal_plus() {
        let (command, arguments, literal) =
            crate::parse::parse_request_line("PUTSCRIPT \"main\" {12+}").unwrap();
        assert_eq!(command, "PUTSCRIPT");
        assert_eq!(crate::parse::as_string(&arguments[0]).unwrap(), "main");
        assert_eq!(literal, Some(12));
    }

    #[tokio::test]
    async fn managesieve_session_supports_put_list_get_and_activate() {
        let store = FakeStore::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = ManageSieveServer::new(store.clone());
        tokio::spawn(async move {
            server.serve(listener).await.unwrap();
        });

        let mut stream = TcpStream::connect(address).await.unwrap();
        let mut greeting = [0_u8; 128];
        let read = stream.read(&mut greeting).await.unwrap();
        assert!(String::from_utf8_lossy(&greeting[..read]).contains("ManageSieve"));

        let auth_payload = BASE64.encode("\0alice@example.test\0secret");
        stream
            .write_all(format!("AUTHENTICATE \"PLAIN\" \"{auth_payload}\"\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = [0_u8; 128];
        let read = stream.read(&mut response).await.unwrap();
        assert!(String::from_utf8_lossy(&response[..read]).contains("OK"));

        stream
            .write_all(b"PUTSCRIPT \"main\" {20+}\r\nkeep; discard; stop;\r\n")
            .await
            .unwrap();
        let read = stream.read(&mut response).await.unwrap();
        assert!(String::from_utf8_lossy(&response[..read]).contains("OK"));

        stream.write_all(b"LISTSCRIPTS\r\n").await.unwrap();
        let read = stream.read(&mut response).await.unwrap();
        assert!(String::from_utf8_lossy(&response[..read]).contains("\"main\""));

        stream.write_all(b"SETACTIVE \"main\"\r\n").await.unwrap();
        let read = stream.read(&mut response).await.unwrap();
        assert!(String::from_utf8_lossy(&response[..read]).contains("OK"));
    }

    #[tokio::test]
    async fn managesieve_accepts_xoauth2() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var(
            "LPE_MAIL_OAUTH_SIGNING_SECRET",
            "0123456789abcdef0123456789abcdef",
        );
        let mut store = FakeStore::new();
        store.session = Some(lpe_storage::AuthenticatedAccount {
            tenant_id: store.login.tenant_id.clone(),
            account_id: store.login.account_id,
            email: store.login.email.clone(),
            display_name: store.login.display_name.clone(),
            expires_at: "2099-01-01T00:00:00Z".to_string(),
        });
        let token = issue_oauth_access_token(
            &lpe_mail_auth::AccountPrincipal {
                tenant_id: store.login.tenant_id.clone(),
                account_id: store.login.account_id,
                email: store.login.email.clone(),
                display_name: store.login.display_name.clone(),
            },
            "managesieve",
            600,
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = ManageSieveServer::new(store.clone());
        tokio::spawn(async move {
            server.serve(listener).await.unwrap();
        });

        let mut stream = TcpStream::connect(address).await.unwrap();
        let mut greeting = [0_u8; 128];
        let _ = stream.read(&mut greeting).await.unwrap();

        let auth_payload = BASE64.encode(format!(
            "user={}\u{1}auth=Bearer {}\u{1}\u{1}",
            store.login.email, token
        ));
        stream
            .write_all(format!("AUTHENTICATE \"XOAUTH2\" \"{auth_payload}\"\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = [0_u8; 128];
        let read = stream.read(&mut response).await.unwrap();
        assert!(String::from_utf8_lossy(&response[..read]).contains("OK"));

        std::env::remove_var("LPE_MAIL_OAUTH_SIGNING_SECRET");
    }
}
