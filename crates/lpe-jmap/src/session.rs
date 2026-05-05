use anyhow::{bail, Result};
use axum::http::HeaderMap;
use serde_json::{json, Value};
use std::collections::HashMap;
use uuid::Uuid;

use lpe_storage::{AuthenticatedAccount, MailboxAccountAccess};

use crate::{
    parse::parse_uuid,
    protocol::{SessionAccount, SessionDocument},
    service::opaque_state_fingerprint,
    JmapService, JMAP_BLOB_CAPABILITY, JMAP_CALENDARS_CAPABILITY, JMAP_CONTACTS_CAPABILITY,
    JMAP_CORE_CAPABILITY, JMAP_MAIL_CAPABILITY, JMAP_SUBMISSION_CAPABILITY, JMAP_TASKS_CAPABILITY,
    JMAP_VACATION_RESPONSE_CAPABILITY, JMAP_WEBSOCKET_CAPABILITY, MAX_BLOB_DATA_SOURCES,
    MAX_CONCURRENT_UPLOAD, MAX_SIZE_UPLOAD, SESSION_STATE,
};

impl<S: crate::store::JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub async fn session_document(
        &self,
        authorization: Option<&str>,
        websocket_url: Option<&str>,
        public_base_path: Option<&str>,
    ) -> Result<SessionDocument> {
        let account = self.authenticate(authorization).await?;
        let capabilities = session_capabilities(websocket_url.unwrap_or("ws://localhost/jmap/ws"));
        let public_base_path = normalize_public_base_path(public_base_path);
        let accessible_accounts = self
            .store
            .fetch_accessible_mailbox_accounts(account.account_id)
            .await?;
        let mut accounts = HashMap::new();
        for accessible in &accessible_accounts {
            accounts.insert(
                accessible.account_id.to_string(),
                SessionAccount {
                    name: accessible.email.clone(),
                    is_personal: accessible.is_owned,
                    is_read_only: mailbox_account_is_read_only(accessible),
                    account_capabilities: session_account_capabilities(accessible, &capabilities),
                },
            );
        }

        let mut primary_accounts = HashMap::new();
        let account_id = account.account_id.to_string();
        primary_accounts.insert(JMAP_CORE_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_MAIL_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_SUBMISSION_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_BLOB_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_CONTACTS_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_CALENDARS_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_TASKS_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(
            JMAP_VACATION_RESPONSE_CAPABILITY.to_string(),
            account_id.clone(),
        );

        Ok(SessionDocument {
            capabilities,
            accounts,
            primary_accounts,
            username: account.email,
            api_url: format!("{public_base_path}/api"),
            download_url: format!("{public_base_path}/download/{{accountId}}/{{blobId}}/{{name}}"),
            upload_url: format!("{public_base_path}/upload/{{accountId}}"),
            event_source_url: None,
            state: session_state(&accessible_accounts),
        })
    }
}

pub(crate) fn public_base_path(headers: &HeaderMap) -> String {
    normalize_public_base_path(
        headers
            .get("x-forwarded-prefix")
            .and_then(|value| value.to_str().ok()),
    )
}

pub(crate) fn websocket_url(headers: &HeaderMap) -> Option<String> {
    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get("host"))
        .and_then(|value| value.to_str().ok())?;
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .map(|value| match value {
            "https" => "wss",
            "http" => "ws",
            other if other.starts_with("ws") => other,
            _ => "ws",
        })
        .unwrap_or("ws");
    let public_base_path = public_base_path(headers);
    Some(format!("{scheme}://{host}{public_base_path}/ws"))
}

fn normalize_public_base_path(value: Option<&str>) -> String {
    let path = value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("/jmap");
    let path = format!("/{}", path.trim_matches('/'));
    if path == "/" {
        "/jmap".to_string()
    } else {
        path
    }
}

fn session_capabilities(websocket_url: &str) -> HashMap<String, Value> {
    HashMap::from([
        (
            JMAP_CORE_CAPABILITY.to_string(),
            json!({
                "maxSizeUpload": MAX_SIZE_UPLOAD,
                "maxCallsInRequest": 16,
                "maxConcurrentUpload": MAX_CONCURRENT_UPLOAD,
                "maxObjectsInGet": 250,
                "maxObjectsInSet": 128,
                "collationAlgorithms": ["i;ascii-casemap"],
            }),
        ),
        (
            JMAP_MAIL_CAPABILITY.to_string(),
            json!({
                "maxMailboxesPerEmail": 1,
                "maxMailboxDepth": 1,
                "emailQuerySortOptions": ["receivedAt"],
            }),
        ),
        (
            JMAP_SUBMISSION_CAPABILITY.to_string(),
            json!({
                "maxDelayedSend": 0,
            }),
        ),
        (JMAP_BLOB_CAPABILITY.to_string(), json!({})),
        (
            JMAP_CONTACTS_CAPABILITY.to_string(),
            json!({
                "maxAddressBooksPerCard": 1,
            }),
        ),
        (
            JMAP_CALENDARS_CAPABILITY.to_string(),
            json!({
                "maxCalendarsPerEvent": 1,
            }),
        ),
        (
            JMAP_TASKS_CAPABILITY.to_string(),
            json!({
                "minDateTime": "1970-01-01T00:00:00",
                "maxDateTime": "9999-12-31T23:59:59",
                "mayCreateTaskList": true,
            }),
        ),
        (JMAP_VACATION_RESPONSE_CAPABILITY.to_string(), json!({})),
        (
            JMAP_WEBSOCKET_CAPABILITY.to_string(),
            json!({
                "url": websocket_url,
                "supportsPush": true,
            }),
        ),
    ])
}

pub(crate) fn requested_account_id(
    requested_account_id: Option<&str>,
    account: &AuthenticatedAccount,
) -> Result<Uuid> {
    match requested_account_id {
        Some(value) => {
            let id = parse_uuid(value)?;
            if id == account.account_id {
                Ok(id)
            } else {
                bail!("accountId does not match authenticated account");
            }
        }
        None => Ok(account.account_id),
    }
}

fn mailbox_account_is_read_only(access: &MailboxAccountAccess) -> bool {
    !access.is_owned && !access.may_write
}

fn session_account_capabilities(
    access: &MailboxAccountAccess,
    capabilities: &HashMap<String, Value>,
) -> HashMap<String, Value> {
    if access.is_owned {
        return capabilities
            .iter()
            .map(|(name, value)| (name.clone(), account_capability_value(access, name, value)))
            .collect();
    }

    let mut account_capabilities = HashMap::new();
    for capability in [
        JMAP_CORE_CAPABILITY,
        JMAP_MAIL_CAPABILITY,
        JMAP_BLOB_CAPABILITY,
        JMAP_WEBSOCKET_CAPABILITY,
    ] {
        if let Some(value) = capabilities.get(capability) {
            account_capabilities.insert(
                capability.to_string(),
                account_capability_value(access, capability, value),
            );
        }
    }
    if access.may_write && (access.may_send_as || access.may_send_on_behalf) {
        if let Some(value) = capabilities.get(JMAP_SUBMISSION_CAPABILITY) {
            account_capabilities.insert(
                JMAP_SUBMISSION_CAPABILITY.to_string(),
                account_capability_value(access, JMAP_SUBMISSION_CAPABILITY, value),
            );
        }
    }
    account_capabilities
}

fn account_capability_value(
    access: &MailboxAccountAccess,
    capability: &str,
    global_value: &Value,
) -> Value {
    if capability == JMAP_BLOB_CAPABILITY {
        json!({
            "maxSizeBlobSet": if access.is_owned || access.may_write { MAX_SIZE_UPLOAD } else { 0 },
            "maxDataSources": MAX_BLOB_DATA_SOURCES,
            "supportedTypeNames": ["Mailbox", "Thread", "Email"],
            "supportedDigestAlgorithms": ["sha-256"],
        })
    } else {
        global_value.clone()
    }
}

pub(crate) fn session_state(accessible_accounts: &[MailboxAccountAccess]) -> String {
    let mut entries = accessible_accounts
        .iter()
        .map(|account| {
            let may_submit = account.may_write
                && (account.is_owned || account.may_send_as || account.may_send_on_behalf);
            format!(
                "{}|{}|{}|{}|{}|{}|{}|{}",
                account.account_id,
                account.email,
                account.display_name,
                account.is_owned,
                account.may_read,
                account.may_write,
                may_submit,
                session_account_version(account)
            )
        })
        .collect::<Vec<_>>();
    entries.sort();
    opaque_state_fingerprint(&format!("{}|{}", SESSION_STATE, entries.join(";")))
}

fn session_account_version(access: &MailboxAccountAccess) -> &'static str {
    if access.is_owned {
        "owned"
    } else if access.may_write && (access.may_send_as || access.may_send_on_behalf) {
        "shared-submit"
    } else if access.may_write {
        "shared-write"
    } else {
        "shared-read"
    }
}
