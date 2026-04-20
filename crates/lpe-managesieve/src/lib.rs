use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lpe_mail_auth::{authenticate_plain_credentials, AccountAuthStore};
use lpe_storage::{
    AuditEntryInput, SieveScriptDocument, SieveScriptSummary, Storage,
};
use std::{future::Future, pin::Pin};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use uuid::Uuid;

pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

pub trait ManageSieveStore: AccountAuthStore {
    fn list_sieve_scripts<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<SieveScriptSummary>>;
    fn get_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
    ) -> StoreFuture<'a, Option<SieveScriptDocument>>;
    fn put_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        content: &'a str,
        activate: bool,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SieveScriptDocument>;
    fn delete_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;
    fn rename_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        old_name: &'a str,
        new_name: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SieveScriptSummary>;
    fn set_active_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: Option<&'a str>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<String>>;
}

impl ManageSieveStore for Storage {
    fn list_sieve_scripts<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<SieveScriptSummary>> {
        Box::pin(async move { self.list_sieve_scripts(account_id).await })
    }

    fn get_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
    ) -> StoreFuture<'a, Option<SieveScriptDocument>> {
        Box::pin(async move { self.get_sieve_script(account_id, name).await })
    }

    fn put_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        content: &'a str,
        activate: bool,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SieveScriptDocument> {
        Box::pin(async move {
            self.put_sieve_script(account_id, name, content, activate, audit)
                .await
        })
    }

    fn delete_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_sieve_script(account_id, name, audit).await })
    }

    fn rename_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        old_name: &'a str,
        new_name: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SieveScriptSummary> {
        Box::pin(async move {
            self.rename_sieve_script(account_id, old_name, new_name, audit)
                .await
        })
    }

    fn set_active_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: Option<&'a str>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<String>> {
        Box::pin(async move { self.set_active_sieve_script(account_id, name, audit).await })
    }
}

#[derive(Clone)]
pub struct ManageSieveServer<S> {
    store: S,
}

impl<S: ManageSieveStore> ManageSieveServer<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn serve(self, listener: TcpListener) -> Result<()> {
        loop {
            let (stream, _) = listener.accept().await?;
            let store = self.store.clone();
            tokio::spawn(async move {
                let _ = handle_connection(store, stream).await;
            });
        }
    }
}

pub async fn serve(listener: TcpListener, store: impl ManageSieveStore) -> Result<()> {
    ManageSieveServer::new(store).serve(listener).await
}

#[derive(Debug, Clone)]
struct AuthenticatedAccount {
    account_id: Uuid,
    email: String,
}

async fn handle_connection<S: ManageSieveStore>(store: S, stream: TcpStream) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    writer
        .write_all(b"OK \"LPE ManageSieve ready\"\r\n")
        .await?;
    let mut authenticated = None;

    loop {
        let request = match read_request(&mut reader).await? {
            Some(request) => request,
            None => return Ok(()),
        };
        let command = request.command.to_ascii_uppercase();
        match command.as_str() {
            "CAPABILITY" => write_capability(&mut writer).await?,
            "AUTHENTICATE" => {
                authenticated = Some(authenticate(&store, &request.arguments).await?);
                writer
                    .write_all(b"OK \"authentication successful\"\r\n")
                    .await?;
            }
            "NOOP" => writer.write_all(b"OK\r\n").await?,
            "LOGOUT" => {
                writer.write_all(b"OK \"logout\"\r\n").await?;
                return Ok(());
            }
            "HAVESPACE" => {
                require_auth(&authenticated)?;
                handle_havespace(&mut writer, &request.arguments).await?;
            }
            "LISTSCRIPTS" => {
                let account = require_auth(&authenticated)?;
                let scripts = store.list_sieve_scripts(account.account_id).await?;
                for script in scripts {
                    if script.is_active {
                        writer
                            .write_all(format!("\"{}\" ACTIVE\r\n", script.name).as_bytes())
                            .await?;
                    } else {
                        writer
                            .write_all(format!("\"{}\"\r\n", script.name).as_bytes())
                            .await?;
                    }
                }
                writer.write_all(b"OK\r\n").await?;
            }
            "GETSCRIPT" => {
                let account = require_auth(&authenticated)?;
                let name = single_string_arg(&request.arguments)?;
                let script = store
                    .get_sieve_script(account.account_id, &name)
                    .await?
                    .ok_or_else(|| anyhow!("script not found"))?;
                writer
                    .write_all(format!("{{{}}}\r\n", script.content.len()).as_bytes())
                    .await?;
                writer.write_all(script.content.as_bytes()).await?;
                writer.write_all(b"\r\nOK\r\n").await?;
            }
            "PUTSCRIPT" => {
                let account = require_auth(&authenticated)?;
                if request.arguments.len() != 2 {
                    bail!("PUTSCRIPT expects name and script literal");
                }
                let name = as_string(&request.arguments[0])?;
                let content = as_string(&request.arguments[1])?;
                store
                    .put_sieve_script(
                        account.account_id,
                        &name,
                        &content,
                        false,
                        AuditEntryInput {
                            actor: account.email.clone(),
                            action: "mail.sieve.put-script".to_string(),
                            subject: name.clone(),
                        },
                    )
                    .await?;
                writer.write_all(b"OK\r\n").await?;
            }
            "CHECKSCRIPT" => {
                require_auth(&authenticated)?;
                let content = single_string_arg(&request.arguments)?;
                lpe_core::sieve::parse_script(&content)?;
                writer.write_all(b"OK\r\n").await?;
            }
            "SETACTIVE" => {
                let account = require_auth(&authenticated)?;
                let name = single_string_arg(&request.arguments)?;
                let active = if name.is_empty() {
                    None
                } else {
                    Some(name.clone())
                };
                store
                    .set_active_sieve_script(
                        account.account_id,
                        active.as_deref(),
                        AuditEntryInput {
                            actor: account.email.clone(),
                            action: "mail.sieve.set-active".to_string(),
                            subject: if name.is_empty() {
                                "<none>".to_string()
                            } else {
                                name
                            },
                        },
                    )
                    .await?;
                writer.write_all(b"OK\r\n").await?;
            }
            "DELETESCRIPT" => {
                let account = require_auth(&authenticated)?;
                let name = single_string_arg(&request.arguments)?;
                store
                    .delete_sieve_script(
                        account.account_id,
                        &name,
                        AuditEntryInput {
                            actor: account.email.clone(),
                            action: "mail.sieve.delete-script".to_string(),
                            subject: name.clone(),
                        },
                    )
                    .await?;
                writer.write_all(b"OK\r\n").await?;
            }
            "RENAMESCRIPT" => {
                let account = require_auth(&authenticated)?;
                if request.arguments.len() != 2 {
                    bail!("RENAMESCRIPT expects old and new names");
                }
                let old_name = as_string(&request.arguments[0])?;
                let new_name = as_string(&request.arguments[1])?;
                store
                    .rename_sieve_script(
                        account.account_id,
                        &old_name,
                        &new_name,
                        AuditEntryInput {
                            actor: account.email.clone(),
                            action: "mail.sieve.rename-script".to_string(),
                            subject: format!("{old_name}->{new_name}"),
                        },
                    )
                    .await?;
                writer.write_all(b"OK\r\n").await?;
            }
            _ => bail!("unsupported ManageSieve command"),
        }
    }
}

async fn authenticate<S: ManageSieveStore>(
    store: &S,
    arguments: &[Argument],
) -> Result<AuthenticatedAccount> {
    if arguments.is_empty() {
        bail!("AUTHENTICATE expects mechanism");
    }
    let mechanism = as_string(&arguments[0])?;
    if mechanism.to_ascii_uppercase() != "PLAIN" {
        bail!("only AUTHENTICATE PLAIN is supported");
    }
    let encoded = if arguments.len() > 1 {
        as_string(&arguments[1])?
    } else {
        bail!("AUTHENTICATE PLAIN requires an initial response");
    };
    let decoded = BASE64.decode(encoded.trim())?;
    let mut parts = decoded.split(|value| *value == 0);
    let _authzid = parts.next();
    let username = String::from_utf8(parts.next().unwrap_or_default().to_vec())?;
    let password = String::from_utf8(parts.next().unwrap_or_default().to_vec())?;
    let principal =
        authenticate_plain_credentials(store, None, &username, &password, "managesieve").await?;
    Ok(AuthenticatedAccount {
        account_id: principal.account_id,
        email: principal.email,
    })
}

async fn write_capability<W: AsyncWriteExt + Unpin>(writer: &mut W) -> Result<()> {
    writer
        .write_all(
            concat!(
                "\"IMPLEMENTATION\" \"LPE ManageSieve\"\r\n",
                "\"SASL\" \"PLAIN\"\r\n",
                "\"SIEVE\" \"fileinto discard redirect vacation\"\r\n",
                "\"VERSION\" \"1.0\"\r\n",
                "OK\r\n"
            )
            .as_bytes(),
        )
        .await?;
    Ok(())
}

async fn handle_havespace<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    arguments: &[Argument],
) -> Result<()> {
    if arguments.len() != 2 {
        bail!("HAVESPACE expects name and size");
    }
    let size = match &arguments[1] {
        Argument::Atom(value) => value.parse::<usize>()?,
        _ => bail!("HAVESPACE size must be numeric"),
    };
    if size > 64 * 1024 {
        writer.write_all(b"NO \"script too large\"\r\n").await?;
    } else {
        writer.write_all(b"OK\r\n").await?;
    }
    Ok(())
}

fn require_auth(account: &Option<AuthenticatedAccount>) -> Result<&AuthenticatedAccount> {
    account
        .as_ref()
        .ok_or_else(|| anyhow!("authentication required"))
}

fn single_string_arg(arguments: &[Argument]) -> Result<String> {
    if arguments.len() != 1 {
        bail!("expected exactly one string argument");
    }
    as_string(&arguments[0])
}

fn as_string(argument: &Argument) -> Result<String> {
    match argument {
        Argument::Atom(value) | Argument::String(value) | Argument::Literal(value) => {
            Ok(value.clone())
        }
    }
}

#[derive(Debug)]
struct Request {
    command: String,
    arguments: Vec<Argument>,
}

#[derive(Debug, Clone)]
enum Argument {
    Atom(String),
    String(String),
    Literal(String),
}

async fn read_request<R: AsyncBufReadExt + AsyncReadExt + Unpin>(
    reader: &mut R,
) -> Result<Option<Request>> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(None);
    }
    let line = line.trim_end_matches(['\r', '\n']);
    if line.is_empty() {
        return Ok(None);
    }
    let (mut command, mut arguments, literal_len) = parse_request_line(line)?;
    if let Some(literal_len) = literal_len {
        let mut bytes = vec![0; literal_len];
        reader.read_exact(&mut bytes).await?;
        let mut crlf = [0_u8; 2];
        reader.read_exact(&mut crlf).await?;
        arguments.push(Argument::Literal(String::from_utf8(bytes)?));
    }
    Ok(Some(Request {
        command: std::mem::take(&mut command),
        arguments,
    }))
}

fn parse_request_line(input: &str) -> Result<(String, Vec<Argument>, Option<usize>)> {
    let mut chars = input.chars().peekable();
    let command = parse_atom(&mut chars)?;
    let mut arguments = Vec::new();
    let mut literal_len = None;
    loop {
        skip_ws(&mut chars);
        let Some(next) = chars.peek().copied() else {
            break;
        };
        match next {
            '"' => arguments.push(Argument::String(parse_quoted(&mut chars)?)),
            '{' => {
                literal_len = Some(parse_literal_marker(&mut chars)?);
                break;
            }
            _ => arguments.push(Argument::Atom(parse_atom(&mut chars)?)),
        }
    }
    Ok((command, arguments, literal_len))
}

fn parse_atom<I>(chars: &mut std::iter::Peekable<I>) -> Result<String>
where
    I: Iterator<Item = char>,
{
    let mut value = String::new();
    while let Some(next) = chars.peek().copied() {
        if next.is_whitespace() {
            break;
        }
        if matches!(next, '"' | '{' | '}') {
            break;
        }
        value.push(next);
        chars.next();
    }
    if value.is_empty() {
        bail!("expected atom");
    }
    Ok(value)
}

fn parse_quoted<I>(chars: &mut std::iter::Peekable<I>) -> Result<String>
where
    I: Iterator<Item = char>,
{
    let mut value = String::new();
    if chars.next() != Some('"') {
        bail!("expected quoted string");
    }
    let mut escaped = false;
    for next in chars.by_ref() {
        if escaped {
            value.push(next);
            escaped = false;
            continue;
        }
        match next {
            '\\' => escaped = true,
            '"' => return Ok(value),
            other => value.push(other),
        }
    }
    bail!("unterminated quoted string")
}

fn parse_literal_marker<I>(chars: &mut std::iter::Peekable<I>) -> Result<usize>
where
    I: Iterator<Item = char>,
{
    if chars.next() != Some('{') {
        bail!("expected literal marker");
    }
    let mut digits = String::new();
    while let Some(next) = chars.peek().copied() {
        if next.is_ascii_digit() {
            digits.push(next);
            chars.next();
        } else {
            break;
        }
    }
    if chars.next() != Some('+') || chars.next() != Some('}') {
        bail!("only non-synchronizing literals are supported");
    }
    digits.parse::<usize>().map_err(Into::into)
}

fn skip_ws<I>(chars: &mut std::iter::Peekable<I>)
where
    I: Iterator<Item = char>,
{
    while matches!(chars.peek(), Some(value) if value.is_whitespace()) {
        chars.next();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use argon2::{
        password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
        Argon2,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct FakeStore {
        login: AccountLogin,
        scripts: Arc<Mutex<Vec<SieveScriptDocument>>>,
        active: Arc<Mutex<Option<String>>>,
    }

    impl FakeStore {
        fn new() -> Self {
            Self {
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

    impl ManageSieveStore for FakeStore {
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
        let (command, arguments, literal) = parse_request_line("PUTSCRIPT \"main\" {12+}").unwrap();
        assert_eq!(command, "PUTSCRIPT");
        assert_eq!(as_string(&arguments[0]).unwrap(), "main");
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
}
