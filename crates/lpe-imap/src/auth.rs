use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lpe_magika::Detector;
use lpe_mail_auth::{authenticate_bearer_access_token, authenticate_plain_credentials};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};

use crate::{parse::tokenize, Session};

impl<S: crate::store::ImapStore, D: Detector> Session<S, D> {
    pub(crate) async fn handle_login<W>(
        &mut self,
        tag: &str,
        arguments: &str,
        writer: &mut W,
    ) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        if self.principal.is_some() {
            bail!("already authenticated");
        }

        let tokens = tokenize(arguments)?;
        if tokens.len() != 2 {
            bail!("LOGIN expects username and password");
        }
        let username = tokens[0].clone();
        let password = tokens[1].clone();
        self.principal = Some(
            authenticate_plain_credentials(&self.store, None, &username, &password, "imap").await?,
        );
        self.selected = None;

        writer
            .write_all(format!("{tag} OK LOGIN completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    pub(crate) async fn handle_authenticate<R, W>(
        &mut self,
        reader: &mut BufReader<R>,
        tag: &str,
        arguments: &str,
        writer: &mut W,
    ) -> Result<bool>
    where
        R: AsyncRead + Unpin,
        W: AsyncWriteExt + Unpin,
    {
        if self.principal.is_some() {
            bail!("already authenticated");
        }

        let tokens = tokenize(arguments)?;
        let mechanism = tokens
            .first()
            .ok_or_else(|| anyhow!("AUTHENTICATE expects a mechanism"))?;
        if mechanism.eq_ignore_ascii_case("XOAUTH2") {
            if tokens.len() != 2 {
                bail!("AUTHENTICATE XOAUTH2 expects an initial response");
            }
            let (username, bearer_token) = parse_xoauth2_initial_response(&tokens[1])?;
            self.principal = Some(
                authenticate_bearer_access_token(
                    &self.store,
                    Some(&username),
                    &bearer_token,
                    "imap",
                )
                .await?,
            );
        } else if mechanism.eq_ignore_ascii_case("PLAIN") {
            if tokens.len() > 2 {
                bail!("AUTHENTICATE PLAIN expects at most one initial response");
            }
            let initial_response = if let Some(response) = tokens.get(1) {
                response.clone()
            } else {
                writer.write_all(b"+ \r\n").await?;
                writer.flush().await?;
                let mut line = String::new();
                if reader.read_line(&mut line).await? == 0 {
                    bail!("AUTHENTICATE PLAIN response missing");
                }
                line.trim_end_matches(['\r', '\n']).to_string()
            };
            let (username, password) = parse_plain_initial_response(&initial_response)?;
            self.principal = Some(
                authenticate_plain_credentials(&self.store, None, &username, &password, "imap")
                    .await?,
            );
        } else {
            bail!("unsupported AUTHENTICATE mechanism");
        }
        self.selected = None;

        writer
            .write_all(format!("{tag} OK AUTHENTICATE completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }
}

fn parse_xoauth2_initial_response(encoded: &str) -> Result<(String, String)> {
    let decoded = BASE64
        .decode(encoded.trim())
        .map_err(|_| anyhow!("invalid XOAUTH2 initial response"))?;
    let decoded = String::from_utf8(decoded).map_err(|_| anyhow!("invalid XOAUTH2 payload"))?;
    let mut username = None;
    let mut bearer_token = None;
    for segment in decoded.split('\u{1}') {
        if let Some(value) = segment.strip_prefix("user=") {
            let value = value.trim();
            if !value.is_empty() {
                username = Some(value.to_string());
            }
        } else if let Some(value) = segment.strip_prefix("auth=Bearer ") {
            let value = value.trim();
            if !value.is_empty() {
                bearer_token = Some(value.to_string());
            }
        }
    }
    Ok((
        username.ok_or_else(|| anyhow!("XOAUTH2 payload is missing the user field"))?,
        bearer_token.ok_or_else(|| anyhow!("XOAUTH2 payload is missing the bearer token"))?,
    ))
}

fn parse_plain_initial_response(encoded: &str) -> Result<(String, String)> {
    if encoded.trim() == "*" {
        bail!("AUTHENTICATE cancelled");
    }
    let decoded = BASE64
        .decode(encoded.trim())
        .map_err(|_| anyhow!("invalid PLAIN initial response"))?;
    let decoded = String::from_utf8(decoded).map_err(|_| anyhow!("invalid PLAIN payload"))?;
    let fields = decoded.split('\0').collect::<Vec<_>>();
    if fields.len() != 3 {
        bail!("invalid PLAIN payload");
    }
    let username = fields[1].trim();
    let password = fields[2];
    if username.is_empty() || password.is_empty() {
        bail!("invalid PLAIN credentials");
    }
    Ok((username.to_string(), password.to_string()))
}
