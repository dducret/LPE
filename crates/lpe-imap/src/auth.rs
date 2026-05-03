use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lpe_magika::Detector;
use lpe_mail_auth::{authenticate_bearer_access_token, authenticate_plain_credentials};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

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
        tag: &str,
        arguments: &str,
        reader: &mut BufReader<R>,
        writer: &mut W,
    ) -> Result<bool>
    where
        R: AsyncReadExt + Unpin,
        W: AsyncWriteExt + Unpin,
    {
        if self.principal.is_some() {
            bail!("already authenticated");
        }

        let tokens = tokenize(arguments)?;
        let Some(mechanism) = tokens.first() else {
            bail!("AUTHENTICATE expects a mechanism");
        };

        if mechanism.eq_ignore_ascii_case("PLAIN") {
            let initial_response = match tokens.len() {
                1 => {
                    writer.write_all(b"+ \r\n").await?;
                    writer.flush().await?;
                    let mut line = String::new();
                    let bytes = reader.read_line(&mut line).await?;
                    if bytes == 0 {
                        bail!("AUTHENTICATE PLAIN response was not received");
                    }
                    line.trim_end_matches(['\r', '\n']).to_string()
                }
                2 => tokens[1].clone(),
                _ => bail!("AUTHENTICATE PLAIN expects an optional initial response"),
            };
            let (username, password) = parse_plain_initial_response(&initial_response)?;
            self.principal = Some(
                authenticate_plain_credentials(&self.store, None, &username, &password, "imap")
                    .await?,
            );
            self.selected = None;

            writer
                .write_all(format!("{tag} OK AUTHENTICATE completed\r\n").as_bytes())
                .await?;
            writer.flush().await?;
            return Ok(true);
        }

        if !mechanism.eq_ignore_ascii_case("XOAUTH2") {
            bail!("only AUTHENTICATE PLAIN and XOAUTH2 are supported");
        }
        if tokens.len() != 2 {
            bail!("AUTHENTICATE XOAUTH2 expects an initial response");
        }
        let (username, bearer_token) = parse_xoauth2_initial_response(&tokens[1])?;
        self.principal = Some(
            authenticate_bearer_access_token(&self.store, Some(&username), &bearer_token, "imap")
                .await?,
        );
        self.selected = None;

        writer
            .write_all(format!("{tag} OK AUTHENTICATE completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }
}

fn parse_plain_initial_response(encoded: &str) -> Result<(String, String)> {
    if encoded.trim() == "=" {
        bail!("AUTHENTICATE PLAIN response is empty");
    }
    let decoded = BASE64
        .decode(encoded.trim())
        .map_err(|_| anyhow!("invalid PLAIN initial response"))?;
    let parts = decoded.split(|byte| *byte == 0).collect::<Vec<_>>();
    if parts.len() != 3 {
        bail!("invalid PLAIN authentication payload");
    }
    let username = String::from_utf8(parts[1].to_vec())
        .map_err(|_| anyhow!("invalid PLAIN username"))?
        .trim()
        .to_string();
    let password =
        String::from_utf8(parts[2].to_vec()).map_err(|_| anyhow!("invalid PLAIN password"))?;
    if username.is_empty() {
        bail!("PLAIN payload is missing the username");
    }
    Ok((username, password))
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
