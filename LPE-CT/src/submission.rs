use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lpe_domain::{
    SmtpSubmissionAuthRequest, SmtpSubmissionAuthResponse, SmtpSubmissionRequest,
    SmtpSubmissionResponse,
};
use reqwest::StatusCode;
use std::{env, fs::File, io::BufReader, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader as TokioBufReader},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{
    rustls::{pki_types::CertificateDer, pki_types::PrivateKeyDer, ServerConfig},
    TlsAcceptor,
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::integration_shared_secret;

#[derive(Debug, Clone)]
struct SubmissionPrincipal {
    account_id: Uuid,
    email: String,
    display_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SmtpAuthFailureKind {
    InvalidCredentials,
    Temporary,
    Permanent,
}

#[derive(Debug, Default)]
struct SubmissionTransaction {
    helo: String,
    mail_from: String,
    rcpt_to: Vec<String>,
}

impl SubmissionTransaction {
    fn reset_message(&mut self) {
        self.mail_from.clear();
        self.rcpt_to.clear();
    }
}

pub(crate) async fn run_submission_listener(
    bind_address: String,
    core_base_url: String,
) -> Result<()> {
    let tls = load_tls_acceptor()?;
    let listener = TcpListener::bind(&bind_address)
        .await
        .with_context(|| format!("unable to bind SMTP submission listener on {bind_address}"))?;
    info!("lpe-ct smtp submission listener active on {bind_address}");
    let client = reqwest::Client::builder().build()?;

    loop {
        let (stream, peer) = listener.accept().await?;
        let tls = tls.clone();
        let client = client.clone();
        let core_base_url = core_base_url.clone();
        tokio::spawn(async move {
            if let Err(error) =
                handle_submission_session(stream, peer, tls, client, core_base_url).await
            {
                warn!(peer = %peer, error = %error, "smtp submission session failed");
            }
        });
    }
}

async fn handle_submission_session(
    stream: TcpStream,
    peer: SocketAddr,
    tls: TlsAcceptor,
    client: reqwest::Client,
    core_base_url: String,
) -> Result<()> {
    if let Some(role) = crate::ha_non_active_role_for_traffic()? {
        let mut stream = stream;
        write_line(
            &mut stream,
            &format!("421 node role {role} is not accepting SMTP submission traffic"),
        )
        .await?;
        return Ok(());
    }

    let tls_stream = tls.accept(stream).await?;
    let (reader, mut writer) = tokio::io::split(tls_stream);
    let mut reader = TokioBufReader::new(reader);
    let mut line = String::new();
    let mut principal: Option<SubmissionPrincipal> = None;
    let mut transaction = SubmissionTransaction::default();

    write_line(&mut writer, "220 LPE-CT ESMTP submission ready").await?;

    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        let command = line.trim_end_matches(['\r', '\n']).to_string();
        let upper = command.to_ascii_uppercase();

        if upper.starts_with("EHLO ") || upper.starts_with("HELO ") {
            transaction.reset_message();
            transaction.helo = command[5.min(command.len())..].trim().to_string();
            write_line(&mut writer, "250-LPE-CT").await?;
            write_line(&mut writer, "250-AUTH PLAIN LOGIN").await?;
            write_line(
                &mut writer,
                &format!("250 SIZE {}", max_message_size_bytes()),
            )
            .await?;
            continue;
        }

        if upper == "NOOP" {
            write_line(&mut writer, "250 ok").await?;
            continue;
        }

        if upper == "RSET" {
            transaction.reset_message();
            write_line(&mut writer, "250 reset").await?;
            continue;
        }

        if upper == "QUIT" {
            write_line(&mut writer, "221 bye").await?;
            return Ok(());
        }

        if upper.starts_with("AUTH ") {
            transaction.reset_message();
            let authenticated = authenticate_smtp_client(
                &client,
                &core_base_url,
                &mut reader,
                &mut writer,
                &command,
            )
            .await;
            match authenticated {
                Ok(value) => {
                    principal = Some(value);
                    write_line(&mut writer, "235 authentication succeeded").await?;
                }
                Err((kind, error)) => {
                    warn!(peer = %peer, error = %error, "smtp submission authentication failed");
                    write_line(&mut writer, smtp_auth_failure_reply(kind)).await?;
                }
            }
            continue;
        }

        if principal.is_none() {
            write_line(&mut writer, "530 authentication required").await?;
            continue;
        }

        if upper.starts_with("MAIL FROM:") {
            transaction.mail_from = normalize_path_argument(&command[10..]);
            transaction.rcpt_to.clear();
            write_line(&mut writer, "250 sender accepted").await?;
            continue;
        }

        if upper.starts_with("RCPT TO:") {
            if transaction.mail_from.is_empty() {
                write_line(&mut writer, "503 send MAIL FROM first").await?;
                continue;
            }
            transaction
                .rcpt_to
                .push(normalize_path_argument(&command[8..]));
            write_line(&mut writer, "250 recipient accepted").await?;
            continue;
        }

        if upper == "DATA" {
            let Some(principal) = principal.as_ref() else {
                write_line(&mut writer, "530 authentication required").await?;
                continue;
            };
            if transaction.mail_from.is_empty() || transaction.rcpt_to.is_empty() {
                write_line(&mut writer, "503 sender and recipient required").await?;
                continue;
            }
            write_line(&mut writer, "354 end with <CRLF>.<CRLF>").await?;
            let data = read_data(&mut reader).await?;
            let request = SmtpSubmissionRequest {
                trace_id: format!("lpe-ct-sub-{}", Uuid::new_v4()),
                helo: transaction.helo.clone(),
                peer: peer.to_string(),
                account_id: principal.account_id,
                account_email: principal.email.clone(),
                account_display_name: principal.display_name.clone(),
                mail_from: transaction.mail_from.clone(),
                rcpt_to: transaction.rcpt_to.clone(),
                raw_message: data,
            };
            match submit_message(&client, &core_base_url, &request).await {
                Ok(response) => {
                    let detail = response.detail.as_deref().unwrap_or("submission accepted");
                    write_line(&mut writer, &format!("250 {detail}")).await?;
                    info!(
                        trace_id = %response.trace_id,
                        peer = %peer,
                        account_id = %principal.account_id,
                        accepted = response.accepted,
                        "smtp submission relayed to lpe core"
                    );
                    transaction.reset_message();
                }
                Err((status, detail)) => {
                    write_line(&mut writer, &smtp_submission_failure_reply(status, &detail))
                        .await?;
                    transaction.reset_message();
                }
            }
            continue;
        }

        write_line(&mut writer, "502 command not implemented").await?;
    }
}

async fn authenticate_smtp_client<R, W>(
    client: &reqwest::Client,
    core_base_url: &str,
    reader: &mut R,
    writer: &mut W,
    command: &str,
) -> std::result::Result<SubmissionPrincipal, (SmtpAuthFailureKind, String)>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let credentials = if command.to_ascii_uppercase().starts_with("AUTH PLAIN") {
        let initial = command.split_whitespace().nth(2).map(str::to_string);
        parse_auth_plain(reader, writer, initial)
            .await
            .map_err(|error| {
                (
                    SmtpAuthFailureKind::InvalidCredentials,
                    sanitize_smtp_text(&error.to_string()),
                )
            })?
    } else if command.to_ascii_uppercase().starts_with("AUTH LOGIN") {
        let initial = command.split_whitespace().nth(2).map(str::to_string);
        parse_auth_login(reader, writer, initial)
            .await
            .map_err(|error| {
                (
                    SmtpAuthFailureKind::InvalidCredentials,
                    sanitize_smtp_text(&error.to_string()),
                )
            })?
    } else {
        return Err((
            SmtpAuthFailureKind::Permanent,
            "unsupported auth mechanism".to_string(),
        ));
    };

    let response = client
        .post(format!(
            "{}/internal/lpe-ct/submission-auth",
            core_base_url.trim_end_matches('/')
        ))
        .header(
            "x-lpe-integration-key",
            integration_shared_secret().map_err(|error| {
                (
                    SmtpAuthFailureKind::Temporary,
                    sanitize_smtp_text(&error.to_string()),
                )
            })?,
        )
        .header("x-trace-id", format!("lpe-ct-auth-{}", Uuid::new_v4()))
        .json(&SmtpSubmissionAuthRequest {
            login: credentials.0,
            password: credentials.1,
        })
        .send()
        .await
        .map_err(|error| {
            (
                SmtpAuthFailureKind::Temporary,
                sanitize_smtp_text(&error.to_string()),
            )
        })?;
    if response.status().is_success() {
        let body: SmtpSubmissionAuthResponse = response.json().await.map_err(|error| {
            (
                SmtpAuthFailureKind::Temporary,
                sanitize_smtp_text(&error.to_string()),
            )
        })?;
        return Ok(SubmissionPrincipal {
            account_id: body.account_id.ok_or_else(|| {
                (
                    SmtpAuthFailureKind::Permanent,
                    "smtp auth response omitted account_id".to_string(),
                )
            })?,
            email: body
                .account_email
                .as_deref()
                .unwrap_or_default()
                .trim()
                .to_lowercase(),
            display_name: body.account_display_name.unwrap_or_default(),
        });
    }
    let status = response.status();
    let detail = response
        .text()
        .await
        .unwrap_or_else(|_| "smtp submission authentication failed".to_string());
    Err((
        classify_auth_failure_status(status),
        sanitize_smtp_text(&detail),
    ))
}

async fn submit_message(
    client: &reqwest::Client,
    core_base_url: &str,
    request: &SmtpSubmissionRequest,
) -> Result<SmtpSubmissionResponse, (StatusCode, String)> {
    let response = client
        .post(format!(
            "{}/internal/lpe-ct/submissions",
            core_base_url.trim_end_matches('/')
        ))
        .header(
            "x-lpe-integration-key",
            integration_shared_secret().map_err(internal_submission_error)?,
        )
        .header("x-trace-id", &request.trace_id)
        .json(request)
        .send()
        .await
        .map_err(internal_submission_error)?;
    let status = response.status();
    if !status.is_success() {
        let detail = response
            .text()
            .await
            .unwrap_or_else(|_| "submission failed".to_string());
        return Err((status, sanitize_smtp_text(&detail)));
    }
    response.json().await.map_err(internal_submission_error)
}

async fn parse_auth_plain<R, W>(
    reader: &mut R,
    writer: &mut W,
    initial_response: Option<String>,
) -> Result<(String, String)>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let encoded = match initial_response {
        Some(value) => value,
        None => {
            write_line(writer, "334").await?;
            read_client_line(reader).await?
        }
    };
    decode_auth_plain(&encoded)
}

async fn parse_auth_login<R, W>(
    reader: &mut R,
    writer: &mut W,
    initial_username: Option<String>,
) -> Result<(String, String)>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let username = match initial_username {
        Some(value) => decode_auth_login_token(&value)?,
        None => {
            write_line(writer, "334 VXNlcm5hbWU6").await?;
            decode_auth_login_token(&read_client_line(reader).await?)?
        }
    };
    write_line(writer, "334 UGFzc3dvcmQ6").await?;
    let password = decode_auth_login_token(&read_client_line(reader).await?)?;
    Ok((username, password))
}

fn decode_auth_plain(value: &str) -> Result<(String, String)> {
    let decoded = BASE64.decode(value.trim())?;
    let parts = decoded
        .splitn(3, |byte| *byte == 0)
        .map(|part| String::from_utf8(part.to_vec()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if parts.len() != 3 {
        bail!("invalid AUTH PLAIN payload");
    }
    let username = parts[1].trim().to_string();
    let password = parts[2].to_string();
    if username.is_empty() || password.is_empty() {
        bail!("missing username or password");
    }
    Ok((username, password))
}

fn decode_auth_login_token(value: &str) -> Result<String> {
    let decoded = BASE64.decode(value.trim())?;
    let token = String::from_utf8(decoded)?.trim().to_string();
    if token.is_empty() {
        bail!("empty auth token");
    }
    Ok(token)
}

async fn read_client_line<R>(reader: &mut R) -> Result<String>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        bail!("client closed during auth exchange");
    }
    Ok(line.trim_end_matches(['\r', '\n']).to_string())
}

async fn read_data<R>(reader: &mut R) -> Result<Vec<u8>>
where
    R: AsyncBufRead + Unpin,
{
    let max_bytes = max_message_size_bytes();
    let mut data = Vec::new();
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line).await? == 0 {
            bail!("client closed during DATA");
        }
        if line == b".\r\n" || line == b".\n" {
            break;
        }
        if line.starts_with(b"..") {
            data.extend_from_slice(&line[1..]);
        } else {
            data.extend_from_slice(&line);
        }
        if data.len() > max_bytes {
            bail!("message exceeds configured maximum size");
        }
    }
    Ok(data)
}

async fn write_line<W>(writer: &mut W, line: &str) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\r\n").await?;
    writer.flush().await?;
    Ok(())
}

fn normalize_path_argument(value: &str) -> String {
    value.trim().trim_matches(['<', '>']).to_lowercase()
}

fn max_message_size_bytes() -> usize {
    env::var("LPE_CT_SUBMISSION_MAX_MESSAGE_SIZE_MB")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(25)
        * 1024
        * 1024
}

fn load_tls_acceptor() -> Result<TlsAcceptor> {
    let cert_path = required_env("LPE_CT_SUBMISSION_TLS_CERT_PATH")?;
    let key_path = required_env("LPE_CT_SUBMISSION_TLS_KEY_PATH")?;
    let certificates = load_certificates(&cert_path)?;
    let key = load_private_key(&key_path)?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, key)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn load_certificates(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let mut reader = BufReader::new(
        File::open(path).with_context(|| format!("unable to open certificate {path}"))?,
    );
    rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| anyhow!("unable to parse certificate {path}: {error}"))
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let mut reader =
        BufReader::new(File::open(path).with_context(|| format!("unable to open key {path}"))?);
    let mut keys = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| anyhow!("unable to parse private key {path}: {error}"))?;
    if let Some(key) = keys.pop() {
        return Ok(PrivateKeyDer::Pkcs8(key));
    }

    let mut reader =
        BufReader::new(File::open(path).with_context(|| format!("unable to reopen key {path}"))?);
    let mut keys = rustls_pemfile::rsa_private_keys(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| anyhow!("unable to parse rsa private key {path}: {error}"))?;
    let Some(key) = keys.pop() else {
        bail!("no private key found in {path}");
    };
    Ok(PrivateKeyDer::Pkcs1(key))
}

fn required_env(name: &str) -> Result<String> {
    let value = env::var(name).map_err(|_| anyhow!("{name} must be set"))?;
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(trimmed)
}

fn sanitize_smtp_text(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_control() && ch != ' ' {
                ' '
            } else {
                ch
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn internal_submission_error(error: impl ToString) -> (StatusCode, String) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        sanitize_smtp_text(&error.to_string()),
    )
}

fn classify_auth_failure_status(status: StatusCode) -> SmtpAuthFailureKind {
    if status.is_server_error()
        || matches!(
            status,
            StatusCode::REQUEST_TIMEOUT
                | StatusCode::TOO_MANY_REQUESTS
                | StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
        )
    {
        SmtpAuthFailureKind::Temporary
    } else if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
        SmtpAuthFailureKind::InvalidCredentials
    } else {
        SmtpAuthFailureKind::Permanent
    }
}

fn smtp_auth_failure_reply(kind: SmtpAuthFailureKind) -> &'static str {
    match kind {
        SmtpAuthFailureKind::InvalidCredentials => "535 authentication credentials invalid",
        SmtpAuthFailureKind::Temporary => "454 temporary authentication failure",
        SmtpAuthFailureKind::Permanent => "535 authentication mechanism rejected",
    }
}

fn smtp_submission_failure_reply(status: StatusCode, detail: &str) -> String {
    if status.is_server_error()
        || matches!(
            status,
            StatusCode::REQUEST_TIMEOUT
                | StatusCode::TOO_MANY_REQUESTS
                | StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
        )
    {
        return format!("451 submission temporarily unavailable ({detail})");
    }

    if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
        return format!("550 submission rejected ({detail})");
    }

    format!("554 submission rejected ({detail})")
}

#[cfg(test)]
mod tests {
    use super::{
        classify_auth_failure_status, decode_auth_login_token, decode_auth_plain,
        sanitize_smtp_text, smtp_auth_failure_reply, smtp_submission_failure_reply, submit_message,
        SmtpAuthFailureKind,
    };
    use crate::env_test_lock;
    use axum::{extract::State, http::HeaderMap, routing::post, Json, Router};
    use lpe_domain::{SmtpSubmissionRequest, SmtpSubmissionResponse};
    use reqwest::StatusCode;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;
    use uuid::Uuid;

    #[derive(Clone, Default)]
    struct Capture {
        trace_id: Arc<Mutex<Option<String>>>,
    }

    #[test]
    fn auth_plain_decodes_username_and_password() {
        let (username, password) =
            decode_auth_plain("AGFsaWNlQGV4YW1wbGUudGVzdABzZWNyZXQ=").unwrap();
        assert_eq!(username, "alice@example.test");
        assert_eq!(password, "secret");
    }

    #[test]
    fn auth_login_token_decodes_base64_value() {
        let value = decode_auth_login_token("YWxpY2VAZXhhbXBsZS50ZXN0").unwrap();
        assert_eq!(value, "alice@example.test");
    }

    #[test]
    fn smtp_error_text_is_sanitized_for_wire_replies() {
        assert_eq!(
            sanitize_smtp_text("bad\nrequest\r\npayload"),
            "bad request payload"
        );
    }

    #[test]
    fn temporary_submission_failures_map_to_451() {
        assert_eq!(
            smtp_submission_failure_reply(StatusCode::SERVICE_UNAVAILABLE, "core unavailable"),
            "451 submission temporarily unavailable (core unavailable)"
        );
    }

    #[test]
    fn permanent_submission_failures_map_to_550_for_authorization_errors() {
        assert_eq!(
            smtp_submission_failure_reply(StatusCode::FORBIDDEN, "delegation denied"),
            "550 submission rejected (delegation denied)"
        );
    }

    #[test]
    fn auth_failures_distinguish_temporary_and_invalid_credentials() {
        assert_eq!(
            classify_auth_failure_status(StatusCode::SERVICE_UNAVAILABLE),
            SmtpAuthFailureKind::Temporary
        );
        assert_eq!(
            classify_auth_failure_status(StatusCode::UNAUTHORIZED),
            SmtpAuthFailureKind::InvalidCredentials
        );
        assert_eq!(
            smtp_auth_failure_reply(SmtpAuthFailureKind::Temporary),
            "454 temporary authentication failure"
        );
    }

    #[tokio::test]
    #[ignore = "env-sensitive"]
    async fn submit_message_posts_trace_header_and_returns_success() {
        let _guard = env_test_lock();

        async fn accept(
            State(capture): State<Capture>,
            headers: HeaderMap,
        ) -> Json<SmtpSubmissionResponse> {
            *capture.trace_id.lock().unwrap() = headers
                .get("x-trace-id")
                .and_then(|value| value.to_str().ok())
                .map(ToString::to_string);
            Json(SmtpSubmissionResponse {
                accepted: true,
                trace_id: "trace-1".to_string(),
                detail: Some("accepted".to_string()),
            })
        }

        let capture = Capture::default();
        let router = Router::new()
            .route("/internal/lpe-ct/submissions", post(accept))
            .with_state(capture.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        std::env::set_var(
            "LPE_INTEGRATION_SHARED_SECRET",
            "0123456789abcdef0123456789abcdef",
        );
        let request = SmtpSubmissionRequest {
            trace_id: "trace-1".to_string(),
            helo: "client.example.test".to_string(),
            peer: "203.0.113.10:12345".to_string(),
            account_id: Uuid::new_v4(),
            account_email: "alice@example.test".to_string(),
            account_display_name: "Alice".to_string(),
            mail_from: "alice@example.test".to_string(),
            rcpt_to: vec!["bob@example.test".to_string()],
            raw_message: b"From: Alice <alice@example.test>\r\nTo: Bob <bob@example.test>\r\nSubject: Hi\r\n\r\nBody\r\n".to_vec(),
        };
        let client = reqwest::Client::builder().build().unwrap();

        let response = submit_message(&client, &format!("http://{address}"), &request)
            .await
            .unwrap();

        assert!(response.accepted);
        assert_eq!(response.trace_id, "trace-1");
        assert_eq!(capture.trace_id.lock().unwrap().as_deref(), Some("trace-1"));
        std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
    }
}
