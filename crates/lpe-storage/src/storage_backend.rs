use anyhow::{anyhow, bail, Result};
use hmac::{Hmac, Mac};
use lpe_domain::utc_from_unix_seconds;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, CONTENT_LENGTH},
    Client, Method, StatusCode, Url,
};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    env, fmt,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

use crate::StoragePoolConfigSummary;

type HmacSha256 = Hmac<Sha256>;

pub(crate) const STORAGE_POOL_KIND_POSTGRES: &str = "postgres";
pub(crate) const STORAGE_POOL_KIND_S3_COMPATIBLE: &str = "s3_compatible";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageBackendSelection {
    Postgres,
    S3Compatible(S3CompatiblePoolConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct S3CompatiblePoolConfig {
    pub(crate) endpoint_url: String,
    pub(crate) bucket: String,
    pub(crate) signing_region: String,
    pub(crate) addressing_style: S3AddressingStyle,
    pub(crate) object_prefix: Option<String>,
    pub(crate) credentials_ref: String,
}

#[derive(Debug, Clone)]
pub(crate) struct S3ObjectStat {
    pub(crate) size_octets: i64,
    pub(crate) content_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageBackendError {
    InvalidConfig(String),
    SecretUnavailable(String),
    AuthFailed(String),
    PermissionDenied(String),
    NotFound(String),
    ChecksumMismatch(String),
    SizeMismatch(String),
    Timeout(String),
    UnreachableEndpoint(String),
    Unavailable(String),
    UnexpectedStatus(String),
}

impl fmt::Display for StorageBackendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message)
            | Self::SecretUnavailable(message)
            | Self::AuthFailed(message)
            | Self::PermissionDenied(message)
            | Self::NotFound(message)
            | Self::ChecksumMismatch(message)
            | Self::SizeMismatch(message)
            | Self::Timeout(message)
            | Self::UnreachableEndpoint(message)
            | Self::Unavailable(message)
            | Self::UnexpectedStatus(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for StorageBackendError {}

#[derive(Debug, Clone)]
struct S3Credentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum S3AddressingStyle {
    Path,
    VirtualHosted,
}

impl S3AddressingStyle {
    fn as_str(self) -> &'static str {
        match self {
            Self::Path => "path",
            Self::VirtualHosted => "virtualHosted",
        }
    }
}

pub(crate) fn normalize_storage_pool_kind(pool_kind: &str) -> Result<&'static str> {
    match pool_kind.trim() {
        STORAGE_POOL_KIND_POSTGRES => Ok(STORAGE_POOL_KIND_POSTGRES),
        STORAGE_POOL_KIND_S3_COMPATIBLE => Ok(STORAGE_POOL_KIND_S3_COMPATIBLE),
        _ => bail!("unsupported storage pool kind"),
    }
}

pub(crate) fn normalize_storage_pool_config(
    pool_kind: &str,
    config: Option<Value>,
) -> Result<Value> {
    match normalize_storage_pool_kind(pool_kind)? {
        STORAGE_POOL_KIND_POSTGRES => normalize_postgres_config(config),
        STORAGE_POOL_KIND_S3_COMPATIBLE => normalize_s3_compatible_config(config),
        _ => unreachable!("storage pool kind already normalized"),
    }
}

pub(crate) fn select_storage_backend(
    pool_kind: &str,
    config: &Value,
) -> Result<StorageBackendSelection> {
    match normalize_storage_pool_kind(pool_kind)? {
        STORAGE_POOL_KIND_POSTGRES => {
            normalize_postgres_config(Some(config.clone()))?;
            Ok(StorageBackendSelection::Postgres)
        }
        STORAGE_POOL_KIND_S3_COMPATIBLE => Ok(StorageBackendSelection::S3Compatible(
            parse_s3_compatible_config(config)?,
        )),
        _ => unreachable!("storage pool kind already normalized"),
    }
}

pub(crate) fn storage_pool_config_summary(
    pool_kind: &str,
    config: &Value,
) -> Result<Option<StoragePoolConfigSummary>> {
    match select_storage_backend(pool_kind, config)? {
        StorageBackendSelection::Postgres => Ok(None),
        StorageBackendSelection::S3Compatible(config) => Ok(Some(StoragePoolConfigSummary {
            endpoint_url: Some(config.endpoint_url),
            bucket: Some(config.bucket),
            signing_region: Some(config.signing_region),
            addressing_style: Some(config.addressing_style.as_str().to_string()),
            object_prefix: config.object_prefix,
            credentials_configured: true,
        })),
    }
}

pub(crate) fn s3_object_key_for_placement(
    config: &S3CompatiblePoolConfig,
    placement_id: Uuid,
) -> String {
    let placement = placement_id.simple().to_string();
    let key = format!("v1/p/{}/{}", &placement[..2], placement_id);
    match &config.object_prefix {
        Some(prefix) => format!("{prefix}/{key}"),
        None => key,
    }
}

pub(crate) async fn s3_put_object(
    config: &S3CompatiblePoolConfig,
    placement_id: Uuid,
    bytes: &[u8],
    expected_sha256: &str,
    expected_size_octets: i64,
) -> Result<S3ObjectStat> {
    let payload_sha256 = sha256_hex(bytes);
    if payload_sha256 != expected_sha256 {
        return Err(StorageBackendError::ChecksumMismatch(
            "storage backend upload refused bytes with unexpected checksum".to_string(),
        )
        .into());
    }
    if bytes.len() as i64 != expected_size_octets {
        return Err(StorageBackendError::SizeMismatch(
            "storage backend upload refused bytes with unexpected size".to_string(),
        )
        .into());
    }

    let key = s3_object_key_for_placement(config, placement_id);
    let credentials = resolve_s3_credentials(&config.credentials_ref)?;
    let url = s3_object_url(config, &key)?;
    let mut headers = BTreeMap::new();
    headers.insert(
        "x-amz-meta-lpe-placement-id".to_string(),
        placement_id.to_string(),
    );
    headers.insert(
        "x-amz-meta-lpe-sha256".to_string(),
        expected_sha256.to_string(),
    );
    headers.insert(
        "x-amz-meta-lpe-size".to_string(),
        expected_size_octets.to_string(),
    );
    let request = signed_s3_request(
        Method::PUT,
        &url,
        config,
        &credentials,
        &payload_sha256,
        headers,
        SystemTime::now(),
    )?;

    let response = Client::new()
        .put(url)
        .headers(request)
        .body(bytes.to_vec())
        .send()
        .await
        .map_err(map_reqwest_error)?;
    ensure_success_status(response.status(), "put")?;
    s3_stat_object(config, placement_id).await
}

pub(crate) async fn s3_read_object(
    config: &S3CompatiblePoolConfig,
    placement_id: Uuid,
) -> Result<Vec<u8>> {
    let key = s3_object_key_for_placement(config, placement_id);
    let credentials = resolve_s3_credentials(&config.credentials_ref)?;
    let url = s3_object_url(config, &key)?;
    let headers = signed_s3_request(
        Method::GET,
        &url,
        config,
        &credentials,
        "UNSIGNED-PAYLOAD",
        BTreeMap::new(),
        SystemTime::now(),
    )?;
    let response = Client::new()
        .get(url)
        .headers(headers)
        .send()
        .await
        .map_err(map_reqwest_error)?;
    ensure_success_status(response.status(), "read")?;
    let bytes = response.bytes().await.map_err(map_reqwest_error)?;
    Ok(bytes.to_vec())
}

pub(crate) async fn s3_stat_object(
    config: &S3CompatiblePoolConfig,
    placement_id: Uuid,
) -> Result<S3ObjectStat> {
    let key = s3_object_key_for_placement(config, placement_id);
    let credentials = resolve_s3_credentials(&config.credentials_ref)?;
    let url = s3_object_url(config, &key)?;
    let headers = signed_s3_request(
        Method::HEAD,
        &url,
        config,
        &credentials,
        "UNSIGNED-PAYLOAD",
        BTreeMap::new(),
        SystemTime::now(),
    )?;
    let response = Client::new()
        .head(url)
        .headers(headers)
        .send()
        .await
        .map_err(map_reqwest_error)?;
    ensure_success_status(response.status(), "stat")?;
    stat_from_headers(response.headers())
}

pub(crate) async fn s3_probe_pool(config: &S3CompatiblePoolConfig) -> Result<()> {
    let credentials = resolve_s3_credentials(&config.credentials_ref)?;
    let url = s3_bucket_url(config)?;
    let headers = signed_s3_request(
        Method::HEAD,
        &url,
        config,
        &credentials,
        "UNSIGNED-PAYLOAD",
        BTreeMap::new(),
        SystemTime::now(),
    )?;
    let response = Client::new()
        .head(url)
        .headers(headers)
        .send()
        .await
        .map_err(map_reqwest_error)?;
    ensure_success_status(response.status(), "probe")
}

pub(crate) fn map_s3_status_error(status: StatusCode, operation: &str) -> StorageBackendError {
    let message = format!("storage backend {operation} failed with HTTP status {status}");
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN if status == StatusCode::UNAUTHORIZED => {
            StorageBackendError::AuthFailed(message)
        }
        StatusCode::FORBIDDEN => StorageBackendError::PermissionDenied(message),
        StatusCode::NOT_FOUND => StorageBackendError::NotFound(message),
        StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS => {
            StorageBackendError::Unavailable(message)
        }
        status if status.is_server_error() => StorageBackendError::Unavailable(message),
        _ => StorageBackendError::UnexpectedStatus(message),
    }
}

fn normalize_postgres_config(config: Option<Value>) -> Result<Value> {
    match config {
        None | Some(Value::Null) => Ok(json!({})),
        Some(Value::Object(map)) if map.is_empty() => Ok(json!({})),
        Some(_) => bail!("postgres storage pools do not accept backend configuration"),
    }
}

fn normalize_s3_compatible_config(config: Option<Value>) -> Result<Value> {
    let config = match config {
        Some(Value::Object(map)) => map,
        Some(Value::Null) | None => bail!("s3_compatible storage pools require configuration"),
        Some(_) => bail!("s3_compatible storage pool configuration must be an object"),
    };
    let parsed = parse_s3_compatible_config_from_map(&config)?;
    Ok(json!({
        "endpointUrl": parsed.endpoint_url,
        "bucket": parsed.bucket,
        "signingRegion": parsed.signing_region,
        "addressingStyle": parsed.addressing_style.as_str(),
        "objectPrefix": parsed.object_prefix,
        "credentialsRef": parsed.credentials_ref,
    }))
}

fn parse_s3_compatible_config(config: &Value) -> Result<S3CompatiblePoolConfig> {
    let Value::Object(map) = config else {
        bail!("s3_compatible storage pool configuration must be an object");
    };
    parse_s3_compatible_config_from_map(map)
}

fn parse_s3_compatible_config_from_map(
    config: &Map<String, Value>,
) -> Result<S3CompatiblePoolConfig> {
    reject_forbidden_or_unknown_fields(config)?;
    let endpoint_url = normalize_endpoint_url(required_string(config, "endpointUrl")?)?;
    let bucket = normalize_bucket(required_string(config, "bucket")?)?;
    let signing_region = normalize_signing_region(required_region(config)?)?;
    let addressing_style = normalize_addressing_style(required_string(config, "addressingStyle")?)?;
    let object_prefix = optional_string(config, "objectPrefix")?
        .map(normalize_object_prefix)
        .transpose()?;
    let credentials_ref = normalize_credentials_ref(required_string(config, "credentialsRef")?)?;
    Ok(S3CompatiblePoolConfig {
        endpoint_url,
        bucket,
        signing_region,
        addressing_style,
        object_prefix,
        credentials_ref,
    })
}

fn reject_forbidden_or_unknown_fields(config: &Map<String, Value>) -> Result<()> {
    const ALLOWED: &[&str] = &[
        "endpointUrl",
        "bucket",
        "signingRegion",
        "region",
        "addressingStyle",
        "objectPrefix",
        "credentialsRef",
    ];
    const FORBIDDEN: &[&str] = &[
        "accessKeyId",
        "secretAccessKey",
        "secretKey",
        "sessionToken",
        "credentials",
    ];
    for key in config.keys() {
        if FORBIDDEN.contains(&key.as_str()) {
            bail!("s3_compatible credentials must be referenced, not stored inline");
        }
        if !ALLOWED.contains(&key.as_str()) {
            bail!("unsupported s3_compatible storage pool configuration field: {key}");
        }
    }
    Ok(())
}

fn required_string<'a>(config: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    config
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("s3_compatible configuration requires {key}"))
}

fn optional_string<'a>(config: &'a Map<String, Value>, key: &str) -> Result<Option<&'a str>> {
    match config.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed))
            }
        }
        Some(_) => bail!("s3_compatible configuration field {key} must be a string"),
    }
}

fn required_region(config: &Map<String, Value>) -> Result<&str> {
    let signing_region = optional_string(config, "signingRegion")?;
    let region = optional_string(config, "region")?;
    match (signing_region, region) {
        (Some(left), Some(right)) if left != right => {
            bail!("s3_compatible signingRegion and region must match when both are set")
        }
        (Some(value), _) | (_, Some(value)) => Ok(value),
        _ => bail!("s3_compatible configuration requires signingRegion"),
    }
}

fn normalize_endpoint_url(value: &str) -> Result<String> {
    let url = Url::parse(value)
        .map_err(|_| anyhow!("s3_compatible endpointUrl must be an absolute URL"))?;
    if !(url.scheme() == "https" || url.scheme() == "http") {
        bail!("s3_compatible endpointUrl must start with http:// or https://");
    }
    if url.host_str().is_none()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
        || has_control_or_whitespace(value)
    {
        bail!("s3_compatible endpointUrl must not contain whitespace, query, or fragment");
    }
    Ok(value.trim_end_matches('/').to_string())
}

fn normalize_bucket(value: &str) -> Result<String> {
    if value.contains('/') || value.contains('\\') || has_control_or_whitespace(value) {
        bail!("s3_compatible bucket must not contain slashes or whitespace");
    }
    Ok(value.to_string())
}

fn normalize_signing_region(value: &str) -> Result<String> {
    if has_control_or_whitespace(value) {
        bail!("s3_compatible signingRegion must not contain whitespace");
    }
    Ok(value.to_string())
}

fn normalize_addressing_style(value: &str) -> Result<S3AddressingStyle> {
    match value {
        "path" | "pathStyle" | "path-style" | "path_style" => Ok(S3AddressingStyle::Path),
        "virtualHosted" | "virtual-hosted" | "virtual_hosted" => {
            Ok(S3AddressingStyle::VirtualHosted)
        }
        _ => bail!("s3_compatible addressingStyle must be path or virtualHosted"),
    }
}

fn normalize_object_prefix(value: &str) -> Result<String> {
    let trimmed = value.trim_matches('/');
    if trimmed.is_empty() {
        bail!("s3_compatible objectPrefix must not be empty when provided");
    }
    if trimmed.contains('\\')
        || trimmed.contains("//")
        || trimmed.split('/').any(|segment| segment == "..")
        || trimmed.contains('?')
        || trimmed.contains('#')
        || has_control_or_whitespace(trimmed)
    {
        bail!("s3_compatible objectPrefix is invalid");
    }
    Ok(trimmed.to_string())
}

fn resolve_s3_credentials(credentials_ref: &str) -> Result<S3Credentials> {
    let prefix = credentials_ref.strip_prefix("env:").ok_or_else(|| {
        StorageBackendError::InvalidConfig(
            "storage backend credentialsRef must use env: secret reference".to_string(),
        )
    })?;
    let access_key_id = env_secret(prefix, "ACCESS_KEY_ID")?;
    let secret_access_key = env_secret(prefix, "SECRET_ACCESS_KEY")?;
    let session_token = env::var(format!("{prefix}_SESSION_TOKEN"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    Ok(S3Credentials {
        access_key_id,
        secret_access_key,
        session_token,
    })
}

fn env_secret(prefix: &str, suffix: &str) -> Result<String> {
    let name = format!("{prefix}_{suffix}");
    env::var(&name)
        .map(|value| value.trim().to_string())
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            StorageBackendError::SecretUnavailable(format!(
                "storage backend secret reference is missing required environment value {name}"
            ))
            .into()
        })
}

fn s3_object_url(config: &S3CompatiblePoolConfig, key: &str) -> Result<Url> {
    let endpoint = Url::parse(&config.endpoint_url).map_err(|_| {
        StorageBackendError::InvalidConfig("storage backend endpointUrl is invalid".to_string())
    })?;
    let encoded_key = percent_encode_path(key);
    match config.addressing_style {
        S3AddressingStyle::Path => {
            let mut url = endpoint;
            url.set_path(&format!(
                "{}/{}",
                percent_encode_segment(&config.bucket),
                encoded_key
            ));
            Ok(url)
        }
        S3AddressingStyle::VirtualHosted => {
            let host = endpoint.host_str().ok_or_else(|| {
                StorageBackendError::InvalidConfig(
                    "storage backend endpointUrl has no host".to_string(),
                )
            })?;
            let authority = if let Some(port) = endpoint.port() {
                format!("{}.{host}:{port}", config.bucket)
            } else {
                format!("{}.{host}", config.bucket)
            };
            let mut url =
                Url::parse(&format!("{}://{authority}", endpoint.scheme())).map_err(|_| {
                    StorageBackendError::InvalidConfig(
                        "storage backend virtual-hosted URL is invalid".to_string(),
                    )
                })?;
            url.set_path(&encoded_key);
            Ok(url)
        }
    }
}

fn s3_bucket_url(config: &S3CompatiblePoolConfig) -> Result<Url> {
    let endpoint = Url::parse(&config.endpoint_url).map_err(|_| {
        StorageBackendError::InvalidConfig("storage backend endpointUrl is invalid".to_string())
    })?;
    match config.addressing_style {
        S3AddressingStyle::Path => {
            let mut url = endpoint;
            url.set_path(&percent_encode_segment(&config.bucket));
            Ok(url)
        }
        S3AddressingStyle::VirtualHosted => {
            let host = endpoint.host_str().ok_or_else(|| {
                StorageBackendError::InvalidConfig(
                    "storage backend endpointUrl has no host".to_string(),
                )
            })?;
            let authority = if let Some(port) = endpoint.port() {
                format!("{}.{host}:{port}", config.bucket)
            } else {
                format!("{}.{host}", config.bucket)
            };
            Url::parse(&format!("{}://{authority}", endpoint.scheme())).map_err(|_| {
                StorageBackendError::InvalidConfig(
                    "storage backend virtual-hosted URL is invalid".to_string(),
                )
                .into()
            })
        }
    }
}

fn signed_s3_request(
    method: Method,
    url: &Url,
    config: &S3CompatiblePoolConfig,
    credentials: &S3Credentials,
    payload_sha256: &str,
    extra_headers: BTreeMap<String, String>,
    now: SystemTime,
) -> Result<HeaderMap> {
    let (date_stamp, amz_date) = s3_timestamp(now)?;
    let host = canonical_host(url)?;
    let mut headers = BTreeMap::new();
    headers.insert("host".to_string(), host);
    headers.insert(
        "x-amz-content-sha256".to_string(),
        payload_sha256.to_string(),
    );
    headers.insert("x-amz-date".to_string(), amz_date);
    if let Some(token) = &credentials.session_token {
        headers.insert("x-amz-security-token".to_string(), token.clone());
    }
    for (key, value) in extra_headers {
        headers.insert(key.to_ascii_lowercase(), value);
    }

    let signed_headers = headers.keys().cloned().collect::<Vec<_>>().join(";");
    let canonical_headers = headers
        .iter()
        .map(|(key, value)| format!("{key}:{}\n", normalize_header_value(value)))
        .collect::<String>();
    let canonical_request = format!(
        "{}\n{}\n\n{}{}\n{}",
        method.as_str(),
        canonical_uri(url),
        canonical_headers,
        signed_headers,
        payload_sha256
    );
    let credential_scope = format!("{date_stamp}/{}/s3/aws4_request", config.signing_region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        headers
            .get("x-amz-date")
            .expect("x-amz-date was inserted before signing"),
        credential_scope,
        sha256_hex(canonical_request.as_bytes())
    );
    let signing_key = s3_signing_key(&credentials.secret_access_key, &date_stamp, config)?;
    let signature = hmac_sha256_hex(&signing_key, string_to_sign.as_bytes())?;
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        credentials.access_key_id, credential_scope, signed_headers, signature
    );

    let mut header_map = HeaderMap::new();
    for (key, value) in headers {
        header_map.insert(
            HeaderName::from_bytes(key.as_bytes()).map_err(|_| {
                StorageBackendError::InvalidConfig("storage backend header name is invalid".into())
            })?,
            HeaderValue::from_str(&value).map_err(|_| {
                StorageBackendError::InvalidConfig("storage backend header value is invalid".into())
            })?,
        );
    }
    header_map.insert(
        reqwest::header::AUTHORIZATION,
        HeaderValue::from_str(&authorization).map_err(|_| {
            StorageBackendError::InvalidConfig(
                "storage backend authorization header is invalid".into(),
            )
        })?,
    );
    Ok(header_map)
}

fn stat_from_headers(headers: &HeaderMap) -> Result<S3ObjectStat> {
    let size_octets = headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| {
            StorageBackendError::SizeMismatch(
                "storage backend stat response did not include object size".to_string(),
            )
        })?;
    let content_sha256 = headers
        .get("x-amz-meta-lpe-sha256")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| value.len() == 64 && value.chars().all(|c| c.is_ascii_hexdigit()))
        .map(|value| value.to_ascii_lowercase())
        .ok_or_else(|| {
            StorageBackendError::ChecksumMismatch(
                "storage backend stat response did not include LPE checksum metadata".to_string(),
            )
        })?;
    Ok(S3ObjectStat {
        size_octets,
        content_sha256,
    })
}

fn ensure_success_status(status: StatusCode, operation: &str) -> Result<()> {
    if status.is_success() {
        Ok(())
    } else {
        Err(map_s3_status_error(status, operation).into())
    }
}

fn map_reqwest_error(error: reqwest::Error) -> StorageBackendError {
    if error.is_timeout() {
        StorageBackendError::Timeout("storage backend request timed out".to_string())
    } else if error.is_connect() {
        StorageBackendError::UnreachableEndpoint(
            "storage backend endpoint was unreachable".to_string(),
        )
    } else if error.is_request() {
        StorageBackendError::Unavailable("storage backend request failed".to_string())
    } else {
        StorageBackendError::UnexpectedStatus("storage backend response failed".to_string())
    }
}

fn s3_signing_key(
    secret_access_key: &str,
    date_stamp: &str,
    config: &S3CompatiblePoolConfig,
) -> Result<Vec<u8>> {
    let date = hmac_sha256(
        format!("AWS4{secret_access_key}").as_bytes(),
        date_stamp.as_bytes(),
    )?;
    let region = hmac_sha256(&date, config.signing_region.as_bytes())?;
    let service = hmac_sha256(&region, b"s3")?;
    hmac_sha256(&service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], payload: &[u8]) -> Result<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| {
        StorageBackendError::InvalidConfig("storage backend signing key is invalid".into())
    })?;
    mac.update(payload);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hmac_sha256_hex(key: &[u8], payload: &[u8]) -> Result<String> {
    hmac_sha256(key, payload).map(|bytes| hex_lower(&bytes))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_lower(&Sha256::digest(bytes))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn canonical_host(url: &Url) -> Result<String> {
    let host = url.host_str().ok_or_else(|| {
        StorageBackendError::InvalidConfig("storage backend URL has no host".to_string())
    })?;
    Ok(if let Some(port) = url.port() {
        format!("{host}:{port}")
    } else {
        host.to_string()
    })
}

fn canonical_uri(url: &Url) -> String {
    let path = url.path();
    if path.is_empty() {
        "/".to_string()
    } else {
        path.to_string()
    }
}

fn normalize_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn percent_encode_path(value: &str) -> String {
    value
        .split('/')
        .map(percent_encode_segment)
        .collect::<Vec<_>>()
        .join("/")
}

fn percent_encode_segment(value: &str) -> String {
    let mut output = String::new();
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'_' | b'.' | b'~') {
            output.push(*byte as char);
        } else {
            output.push('%');
            output.push_str(&hex_lower(&[*byte]).to_ascii_uppercase());
        }
    }
    output
}

fn s3_timestamp(now: SystemTime) -> Result<(String, String)> {
    let duration = now.duration_since(UNIX_EPOCH).map_err(|_| {
        StorageBackendError::InvalidConfig("storage backend clock is invalid".into())
    })?;
    let date = utc_from_unix_seconds(duration.as_secs());
    let date_stamp = format!("{:04}{:02}{:02}", date.year, date.month, date.day);
    let amz_date = format!(
        "{date_stamp}T{:02}{:02}{:02}Z",
        date.hour, date.minute, date.second
    );
    Ok((date_stamp, amz_date))
}

fn normalize_credentials_ref(value: &str) -> Result<String> {
    if !value.starts_with("env:") {
        bail!("s3_compatible credentialsRef must use an env: deployment secret reference");
    }
    let name = value.trim_start_matches("env:");
    if name.is_empty() || has_control_or_whitespace(name) {
        bail!("s3_compatible credentialsRef is invalid");
    }
    Ok(value.to_string())
}

fn has_control_or_whitespace(value: &str) -> bool {
    value
        .chars()
        .any(|character| character.is_control() || matches!(character, ' ' | '\t' | '\n' | '\r'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn postgres_backend_accepts_empty_config_only() {
        let config = normalize_storage_pool_config(STORAGE_POOL_KIND_POSTGRES, None)
            .expect("postgres config");
        assert_eq!(config, json!({}));
        assert!(matches!(
            select_storage_backend(STORAGE_POOL_KIND_POSTGRES, &config).expect("postgres backend"),
            StorageBackendSelection::Postgres
        ));

        let error = normalize_storage_pool_config(
            STORAGE_POOL_KIND_POSTGRES,
            Some(json!({"endpointUrl": "https://objects.example.test"})),
        )
        .expect_err("postgres config must reject backend fields")
        .to_string();
        assert!(error.contains("do not accept backend configuration"));
    }

    #[test]
    fn s3_compatible_backend_normalizes_provider_neutral_config() {
        let config = normalize_storage_pool_config(
            STORAGE_POOL_KIND_S3_COMPATIBLE,
            Some(json!({
                "endpointUrl": "https://objects.example.test/",
                "bucket": "lpe-blobs",
                "region": "local",
                "addressingStyle": "path-style",
                "objectPrefix": "/mail/blobs/",
                "credentialsRef": "env:LPE_STORAGE_POOL_MAIN"
            })),
        )
        .expect("s3 config");

        assert_eq!(config["endpointUrl"], "https://objects.example.test");
        assert_eq!(config["signingRegion"], "local");
        assert_eq!(config["addressingStyle"], "path");
        assert_eq!(config["objectPrefix"], "mail/blobs");

        let StorageBackendSelection::S3Compatible(selected) =
            select_storage_backend(STORAGE_POOL_KIND_S3_COMPATIBLE, &config).expect("s3 backend")
        else {
            panic!("expected s3-compatible backend");
        };
        assert_eq!(selected.bucket, "lpe-blobs");
        assert_eq!(selected.credentials_ref, "env:LPE_STORAGE_POOL_MAIN");
    }

    #[test]
    fn s3_compatible_backend_rejects_inline_credentials() {
        let error = normalize_storage_pool_config(
            STORAGE_POOL_KIND_S3_COMPATIBLE,
            Some(json!({
                "endpointUrl": "https://objects.example.test",
                "bucket": "lpe-blobs",
                "signingRegion": "local",
                "addressingStyle": "path",
                "accessKeyId": "inline",
                "credentialsRef": "env:LPE_STORAGE_POOL_MAIN"
            })),
        )
        .expect_err("inline credentials must be rejected")
        .to_string();
        assert!(error.contains("not stored inline"));
    }

    #[test]
    fn s3_compatible_summary_redacts_secret_reference() {
        let config = normalize_storage_pool_config(
            STORAGE_POOL_KIND_S3_COMPATIBLE,
            Some(json!({
                "endpointUrl": "https://objects.example.test",
                "bucket": "lpe-blobs",
                "signingRegion": "local",
                "addressingStyle": "virtualHosted",
                "credentialsRef": "env:LPE_STORAGE_POOL_MAIN"
            })),
        )
        .expect("s3 config");

        let summary = storage_pool_config_summary(STORAGE_POOL_KIND_S3_COMPATIBLE, &config)
            .expect("redacted summary")
            .expect("s3 summary");
        assert_eq!(summary.bucket.as_deref(), Some("lpe-blobs"));
        assert!(summary.credentials_configured);
        let serialized = serde_json::to_string(&summary).expect("serialize summary");
        assert!(!serialized.contains("LPE_STORAGE_POOL_MAIN"));
        assert!(!serialized.contains("credentialsRef"));
    }

    #[test]
    fn object_key_is_deterministic_and_omits_tenant_domain_material() {
        let config = S3CompatiblePoolConfig {
            endpoint_url: "https://objects.example.test".to_string(),
            bucket: "lpe-blobs".to_string(),
            signing_region: "local".to_string(),
            addressing_style: S3AddressingStyle::Path,
            object_prefix: Some("mail/blobs".to_string()),
            credentials_ref: "env:LPE_STORAGE_POOL_MAIN".to_string(),
        };
        let placement_id =
            Uuid::parse_str("11111111-2222-3333-4444-555555555555").expect("placement id");

        let key = s3_object_key_for_placement(&config, placement_id);

        assert_eq!(
            key,
            "mail/blobs/v1/p/11/11111111-2222-3333-4444-555555555555"
        );
        assert!(!key.contains("tenant"));
        assert!(!key.contains("domain"));
        assert!(!key.contains('@'));
    }

    #[test]
    fn s3_status_errors_are_storage_backend_errors() {
        let not_found = map_s3_status_error(StatusCode::NOT_FOUND, "read").to_string();
        assert!(not_found.contains("storage backend read failed"));
        assert!(!not_found.contains("mailbox"));
        assert!(!not_found.contains("message"));

        assert!(matches!(
            map_s3_status_error(StatusCode::FORBIDDEN, "stat"),
            StorageBackendError::PermissionDenied(_)
        ));
        assert!(matches!(
            map_s3_status_error(StatusCode::INTERNAL_SERVER_ERROR, "put"),
            StorageBackendError::Unavailable(_)
        ));
    }

    #[test]
    fn s3_signing_timestamp_uses_utc_amz_format() {
        let (date_stamp, amz_date) = s3_timestamp(UNIX_EPOCH).expect("timestamp");
        assert_eq!(date_stamp, "19700101");
        assert_eq!(amz_date, "19700101T000000Z");
    }
}
