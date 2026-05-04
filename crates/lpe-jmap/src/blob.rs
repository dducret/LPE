use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lpe_magika::{ExpectedKind, IngressContext, PolicyDecision, ValidationRequest};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};

use lpe_storage::{AuthenticatedAccount, JmapEmail, MailboxAccountAccess};

use crate::{
    blob_id_for_message,
    error::{method_error, set_error},
    resolve_creation_reference, JmapService, JMAP_MAIL_CAPABILITY, MAX_BLOB_DATA_SOURCES,
    MAX_SIZE_UPLOAD,
};

const DEFAULT_BLOB_MEDIA_TYPE: &str = "application/octet-stream";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BlobCopyArguments {
    account_id: String,
    from_account_id: String,
    blob_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BlobUploadArguments {
    account_id: Option<String>,
    create: HashMap<String, BlobUploadObject>,
}

#[derive(Debug, Deserialize)]
struct BlobUploadObject {
    #[serde(default)]
    data: Vec<BlobDataSource>,
    #[serde(rename = "type")]
    media_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BlobDataSource {
    #[serde(rename = "data:asText")]
    data_as_text: Option<String>,
    #[serde(rename = "data:asBase64")]
    data_as_base64: Option<String>,
    #[serde(rename = "blobId")]
    blob_id: Option<String>,
    offset: Option<u64>,
    length: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BlobGetArguments {
    account_id: Option<String>,
    ids: Option<Vec<String>>,
    properties: Option<Vec<String>>,
    offset: Option<u64>,
    length: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BlobLookupArguments {
    account_id: Option<String>,
    type_names: Vec<String>,
    ids: Vec<String>,
}

impl<S: crate::store::JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) async fn handle_blob_upload(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: BlobUploadArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        ensure_blob_create_allowed(&account_access)?;
        let mut created = Map::new();
        let mut not_created = Map::new();

        for (creation_id, upload) in arguments.create {
            match self
                .build_blob_upload(&account_access, upload, created_ids)
                .await
            {
                Ok((media_type, blob_bytes)) => {
                    match self
                        .store
                        .save_jmap_upload_blob(account_access.account_id, &media_type, &blob_bytes)
                        .await
                    {
                        Ok(blob) => {
                            let blob_id = format!("upload:{}", blob.id);
                            created_ids.insert(creation_id.clone(), blob_id.clone());
                            created.insert(
                                creation_id,
                                json!({
                                    "id": blob_id,
                                    "type": blob.media_type,
                                    "size": blob.octet_size,
                                }),
                            );
                        }
                        Err(error) => {
                            not_created.insert(creation_id, set_error(&error.to_string()));
                        }
                    }
                }
                Err(error) => {
                    not_created.insert(creation_id, set_error(&error.to_string()));
                }
            }
        }

        Ok(json!({
            "accountId": account_access.account_id.to_string(),
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
        }))
    }

    pub(crate) async fn handle_blob_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: BlobGetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let properties = arguments
            .properties
            .unwrap_or_else(|| vec!["data".to_string(), "size".to_string()]);
        let unsupported_digest = properties
            .iter()
            .find(|property| property.starts_with("digest:"));
        if let Some(property) = unsupported_digest {
            bail!("{property} is not supported");
        }
        let mut list = Vec::new();
        let mut not_found = Vec::new();
        for id in arguments.ids.unwrap_or_default() {
            let resolved_id = resolve_creation_reference(&id, created_ids);
            match self
                .resolve_download_blob(&account_access, &resolved_id)
                .await
            {
                Ok(blob) => {
                    list.push(blob_get_object(
                        &resolved_id,
                        &blob.blob_bytes,
                        &properties,
                        arguments.offset.unwrap_or(0),
                        arguments.length,
                    )?);
                }
                Err(_) => not_found.push(id),
            }
        }

        Ok(json!({
            "accountId": account_access.account_id.to_string(),
            "list": list,
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_blob_lookup(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &HashMap<String, String>,
        declared_capabilities: &[String],
    ) -> Result<Value> {
        let arguments: BlobLookupArguments = serde_json::from_value(arguments)?;
        let unknown_type = arguments
            .type_names
            .iter()
            .find(|type_name| !matches!(type_name.as_str(), "Mailbox" | "Thread" | "Email"));
        if let Some(type_name) = unknown_type {
            return Ok(method_error(
                "unknownDataType",
                &format!("{type_name} is not supported for Blob/lookup"),
            ));
        }
        if arguments
            .type_names
            .iter()
            .any(|type_name| matches!(type_name.as_str(), "Mailbox" | "Thread" | "Email"))
            && !declared_capabilities
                .iter()
                .any(|capability| capability == JMAP_MAIL_CAPABILITY)
        {
            return Ok(method_error(
                "unknownDataType",
                "mail capability is required for Blob/lookup mail references",
            ));
        }
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let emails = self
            .store
            .fetch_jmap_emails(
                account_access.account_id,
                &self
                    .store
                    .fetch_all_jmap_email_ids(account_access.account_id)
                    .await?,
            )
            .await?;
        let lookup = blob_lookup_index(&emails);
        let mut list = Vec::new();
        for id in arguments.ids {
            let resolved_id = resolve_creation_reference(&id, created_ids);
            let references = lookup.get(&resolved_id);
            let mut matched_ids = Map::new();
            for type_name in &arguments.type_names {
                let matches = references
                    .map(|entry| match type_name.as_str() {
                        "Mailbox" => sorted_values(&entry.mailbox_ids),
                        "Thread" => sorted_values(&entry.thread_ids),
                        "Email" => sorted_values(&entry.email_ids),
                        _ => Vec::new(),
                    })
                    .unwrap_or_default();
                matched_ids.insert(type_name.clone(), json!(matches));
            }
            list.push(json!({
                "id": resolved_id,
                "matchedIds": Value::Object(matched_ids),
            }));
        }

        Ok(json!({
            "accountId": account_access.account_id.to_string(),
            "list": list,
            "notFound": [],
        }))
    }

    pub(crate) async fn handle_blob_copy(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: BlobCopyArguments = serde_json::from_value(arguments)?;
        let target_account = self
            .requested_account_access(account, Some(&arguments.account_id))
            .await?;
        let source_account = self
            .requested_account_access(account, Some(&arguments.from_account_id))
            .await?;
        ensure_blob_create_allowed(&target_account)?;
        let mut copied = Map::new();
        let mut not_copied = Map::new();

        for blob_id in arguments.blob_ids {
            let resolved_blob_id = resolve_creation_reference(&blob_id, created_ids);
            let include_bcc = source_account.is_owned && target_account.is_owned;
            match self
                .resolve_download_blob_with_bcc(&source_account, &resolved_blob_id, include_bcc)
                .await
            {
                Ok(blob) => {
                    match self
                        .store
                        .save_jmap_upload_blob(
                            target_account.account_id,
                            &blob.media_type,
                            &blob.blob_bytes,
                        )
                        .await
                    {
                        Ok(copied_blob) => {
                            copied.insert(
                                blob_id,
                                Value::String(format!("upload:{}", copied_blob.id)),
                            );
                        }
                        Err(error) => {
                            not_copied.insert(blob_id, set_error(&error.to_string()));
                        }
                    }
                }
                Err(error) => {
                    not_copied.insert(blob_id, set_error(&error.to_string()));
                }
            }
        }

        Ok(json!({
            "fromAccountId": source_account.account_id.to_string(),
            "accountId": target_account.account_id.to_string(),
            "copied": Value::Object(copied),
            "notCopied": Value::Object(not_copied),
        }))
    }

    async fn build_blob_upload(
        &self,
        account_access: &MailboxAccountAccess,
        upload: BlobUploadObject,
        created_ids: &HashMap<String, String>,
    ) -> Result<(String, Vec<u8>)> {
        if upload.data.len() as u64 > MAX_BLOB_DATA_SOURCES {
            bail!("too many blob data sources");
        }

        let mut blob_bytes = Vec::new();
        for source in upload.data {
            let bytes = self
                .resolve_upload_source(account_access, source, created_ids)
                .await?;
            blob_bytes.extend(bytes);
            if blob_bytes.len() as u64 > MAX_SIZE_UPLOAD {
                bail!("blob exceeds maxSizeBlobSet");
            }
        }

        let declared_media_type = upload
            .media_type
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let outcome = self.validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::JmapUpload,
                declared_mime: declared_media_type.map(ToString::to_string),
                filename: None,
                expected_kind: ExpectedKind::Any,
            },
            &blob_bytes,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            bail!(
                "JMAP Blob/upload blocked by Magika validation: {}",
                outcome.reason
            );
        }
        let media_type = declared_media_type
            .map(ToString::to_string)
            .or_else(|| {
                (!outcome.detected_mime.trim().is_empty()).then(|| outcome.detected_mime.clone())
            })
            .unwrap_or_else(|| DEFAULT_BLOB_MEDIA_TYPE.to_string());

        Ok((media_type, blob_bytes))
    }

    async fn resolve_upload_source(
        &self,
        account_access: &MailboxAccountAccess,
        source: BlobDataSource,
        created_ids: &HashMap<String, String>,
    ) -> Result<Vec<u8>> {
        let selected = source.data_as_text.is_some() as u8
            + source.data_as_base64.is_some() as u8
            + source.blob_id.is_some() as u8;
        if selected != 1 {
            bail!("exactly one blob data source is required");
        }

        if let Some(text) = source.data_as_text {
            return Ok(text.into_bytes());
        }
        if let Some(encoded) = source.data_as_base64 {
            return BASE64.decode(encoded).map_err(anyhow::Error::msg);
        }
        let blob_id = resolve_creation_reference(
            source
                .blob_id
                .as_deref()
                .ok_or_else(|| anyhow!("blobId is required"))?,
            created_ids,
        );
        let blob = self.resolve_download_blob(account_access, &blob_id).await?;
        slice_blob_range(&blob.blob_bytes, source.offset.unwrap_or(0), source.length)
    }
}

fn ensure_blob_create_allowed(access: &MailboxAccountAccess) -> Result<()> {
    if access.is_owned || access.may_write {
        Ok(())
    } else {
        bail!("accountId is read-only")
    }
}

#[derive(Debug, Default)]
struct BlobLookupEntry {
    mailbox_ids: HashSet<String>,
    thread_ids: HashSet<String>,
    email_ids: HashSet<String>,
}

fn blob_lookup_index(emails: &[JmapEmail]) -> HashMap<String, BlobLookupEntry> {
    let mut lookup = HashMap::new();
    for email in emails {
        let blob_id = blob_id_for_message(email);
        let entry = lookup
            .entry(blob_id)
            .or_insert_with(BlobLookupEntry::default);
        entry.email_ids.insert(email.id.to_string());
        entry.thread_ids.insert(email.thread_id.to_string());
        entry.mailbox_ids.insert(email.mailbox_id.to_string());
    }
    lookup
}

fn sorted_values(values: &HashSet<String>) -> Vec<String> {
    let mut values = values.iter().cloned().collect::<Vec<_>>();
    values.sort();
    values
}

fn slice_blob_range(bytes: &[u8], offset: u64, length: Option<u64>) -> Result<Vec<u8>> {
    let offset = usize::try_from(offset).map_err(|_| anyhow!("blob range is too large"))?;
    if offset > bytes.len() {
        bail!("blob range is outside the source blob");
    }
    let remaining = bytes.len() - offset;
    let length = length
        .map(|value| usize::try_from(value).map_err(|_| anyhow!("blob range is too large")))
        .transpose()?
        .unwrap_or(remaining);
    let end = offset
        .checked_add(length)
        .ok_or_else(|| anyhow!("blob range is too large"))?;
    if end > bytes.len() {
        bail!("blob range is outside the source blob");
    }
    Ok(bytes[offset..end].to_vec())
}

fn blob_get_object(
    id: &str,
    bytes: &[u8],
    properties: &[String],
    offset: u64,
    length: Option<u64>,
) -> Result<Value> {
    let offset = usize::try_from(offset).map_err(|_| anyhow!("blob range is too large"))?;
    let size = bytes.len();
    let (selected, is_truncated) = readable_blob_range(bytes, offset, length)?;
    let mut object = Map::new();
    object.insert("id".to_string(), Value::String(id.to_string()));

    for property in properties {
        match property.as_str() {
            "size" => {
                object.insert("size".to_string(), json!(size));
            }
            "data" => match std::str::from_utf8(selected) {
                Ok(text) => {
                    object.insert("data:asText".to_string(), Value::String(text.to_string()));
                }
                Err(_) => {
                    object.insert("isEncodingProblem".to_string(), Value::Bool(true));
                    object.insert(
                        "data:asBase64".to_string(),
                        Value::String(BASE64.encode(selected)),
                    );
                }
            },
            "data:asText" => match std::str::from_utf8(selected) {
                Ok(text) => {
                    object.insert("data:asText".to_string(), Value::String(text.to_string()));
                }
                Err(_) => {
                    object.insert("isEncodingProblem".to_string(), Value::Bool(true));
                    object.insert("data:asText".to_string(), Value::Null);
                }
            },
            "data:asBase64" => {
                object.insert(
                    "data:asBase64".to_string(),
                    Value::String(BASE64.encode(selected)),
                );
            }
            _ => {}
        }
    }

    if is_truncated {
        object.insert("isTruncated".to_string(), Value::Bool(true));
    }
    Ok(Value::Object(object))
}

fn readable_blob_range(bytes: &[u8], offset: usize, length: Option<u64>) -> Result<(&[u8], bool)> {
    if offset >= bytes.len() {
        return Ok((&[], offset > bytes.len() || length.unwrap_or(0) > 0));
    }
    let remaining = bytes.len() - offset;
    let requested = length
        .map(|value| usize::try_from(value).map_err(|_| anyhow!("blob range is too large")))
        .transpose()?
        .unwrap_or(remaining);
    let available = requested.min(remaining);
    let is_truncated = requested > remaining;
    Ok((&bytes[offset..offset + available], is_truncated))
}
