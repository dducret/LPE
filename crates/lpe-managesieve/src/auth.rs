use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lpe_mail_auth::{
    authenticate_bearer_access_token, authenticate_plain_credentials,
};
use uuid::Uuid;

use crate::{
    parse::{as_string, Argument},
    store::ManageSieveStore,
};

#[derive(Debug, Clone)]
pub(crate) struct AuthenticatedAccount {
    pub(crate) account_id: Uuid,
    pub(crate) email: String,
}

pub(crate) async fn authenticate<S: ManageSieveStore>(
    store: &S,
    arguments: &[Argument],
) -> Result<AuthenticatedAccount> {
    if arguments.is_empty() {
        bail!("AUTHENTICATE expects mechanism");
    }
    let mechanism = as_string(&arguments[0])?;
    let mechanism = mechanism.to_ascii_uppercase();
    if arguments.len() <= 1 {
        bail!("AUTHENTICATE requires an initial response");
    }
    let encoded = as_string(&arguments[1])?;
    let principal = match mechanism.as_str() {
        "PLAIN" => {
            let decoded = BASE64.decode(encoded.trim())?;
            let mut parts = decoded.split(|value| *value == 0);
            let _authzid = parts.next();
            let username = String::from_utf8(parts.next().unwrap_or_default().to_vec())?;
            let password = String::from_utf8(parts.next().unwrap_or_default().to_vec())?;
            authenticate_plain_credentials(store, None, &username, &password, "managesieve").await?
        }
        "XOAUTH2" => {
            let (username, bearer_token) = parse_xoauth2_initial_response(&encoded)?;
            authenticate_bearer_access_token(store, Some(&username), &bearer_token, "managesieve")
                .await?
        }
        _ => bail!("only AUTHENTICATE PLAIN and XOAUTH2 are supported"),
    };
    Ok(AuthenticatedAccount {
        account_id: principal.account_id,
        email: principal.email,
    })
}

pub(crate) fn require_auth(
    account: &Option<AuthenticatedAccount>,
) -> Result<&AuthenticatedAccount> {
    account
        .as_ref()
        .ok_or_else(|| anyhow!("authentication required"))
}

pub(crate) fn parse_xoauth2_initial_response(encoded: &str) -> Result<(String, String)> {
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
