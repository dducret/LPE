use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use std::{
    collections::hash_map::DefaultHasher,
    env,
    fs::{self, OpenOptions},
    hash::{Hash, Hasher},
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

const DEFAULT_TRACE_DIR: &str = "/opt/lpe/logs/outlook-traces";
const MAX_SANITIZED_PAYLOAD_BYTES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutlookTraceConfig {
    pub enabled: bool,
    pub raw_payloads: bool,
    pub directory: PathBuf,
}

impl OutlookTraceConfig {
    pub fn from_env() -> Self {
        Self {
            enabled: env_flag("LPE_OUTLOOK_TRACE_ENABLED"),
            raw_payloads: env_flag("LPE_OUTLOOK_TRACE_RAW_PAYLOADS"),
            directory: env::var("LPE_OUTLOOK_TRACE_DIR")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(DEFAULT_TRACE_DIR)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutlookTraceDirection {
    Inbound,
    Outbound,
}

impl OutlookTraceDirection {
    fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        }
    }
}

#[derive(Debug)]
pub struct OutlookTraceEvent<'a> {
    pub component: &'a str,
    pub endpoint: &'a str,
    pub session_key: &'a str,
    pub direction: OutlookTraceDirection,
    pub phase: &'a str,
    pub remote_peer: Option<&'a str>,
    pub tenant_id: Option<&'a str>,
    pub account: Option<&'a str>,
    pub status: Option<u16>,
    pub metadata: Vec<(&'a str, String)>,
    pub payload: Option<&'a [u8]>,
}

pub fn write_outlook_trace(event: &OutlookTraceEvent<'_>) {
    let config = OutlookTraceConfig::from_env();
    if config.enabled {
        write_outlook_trace_with_config(&config, event);
    }
}

pub fn write_outlook_trace_with_config(config: &OutlookTraceConfig, event: &OutlookTraceEvent<'_>) {
    if !config.enabled {
        return;
    }
    if let Err(error) = write_event(config, event) {
        tracing::warn!(error = %error, "outlook diagnostic trace write failed");
    }
}

fn write_event(config: &OutlookTraceConfig, event: &OutlookTraceEvent<'_>) -> std::io::Result<()> {
    create_trace_dir(&config.directory)?;
    let path = trace_file_path(&config.directory, event.component, event.session_key);
    let mut file = open_trace_file(&path)?;
    file.write_all(render_event(config, event).as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

fn create_trace_dir(path: &Path) -> std::io::Result<()> {
    fs::create_dir_all(path)?;
    set_restrictive_dir_permissions(path)
}

#[cfg(unix)]
fn set_restrictive_dir_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_restrictive_dir_permissions(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

fn open_trace_file(path: &Path) -> std::io::Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    open_trace_file_with_mode(options, path)
}

#[cfg(unix)]
fn open_trace_file_with_mode(
    mut options: OpenOptions,
    path: &Path,
) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    options.mode(0o600).open(path)
}

#[cfg(not(unix))]
fn open_trace_file_with_mode(options: OpenOptions, path: &Path) -> std::io::Result<std::fs::File> {
    options.open(path)
}

fn trace_file_path(directory: &Path, component: &str, session_key: &str) -> PathBuf {
    directory.join(format!(
        "outlook-{}-{:016x}.jsonl",
        safe_component(component),
        stable_hash(session_key)
    ))
}

fn render_event(config: &OutlookTraceConfig, event: &OutlookTraceEvent<'_>) -> String {
    let mut fields = vec![
        json_pair(
            "timestamp_unix_ms",
            unix_timestamp_millis().to_string(),
            false,
        ),
        json_pair("event_id", Uuid::new_v4().to_string(), true),
        json_pair("component", event.component.to_string(), true),
        json_pair("endpoint", event.endpoint.to_string(), true),
        json_pair(
            "session_id",
            format!("{:016x}", stable_hash(event.session_key)),
            true,
        ),
        json_pair("direction", event.direction.as_str().to_string(), true),
        json_pair("phase", event.phase.to_string(), true),
    ];
    if let Some(remote_peer) = event.remote_peer {
        fields.push(json_pair("remote_peer", remote_peer.to_string(), true));
    }
    if let Some(tenant_id) = event.tenant_id {
        fields.push(json_pair("tenant_id", tenant_id.to_string(), true));
    }
    if let Some(account) = event.account {
        fields.push(json_pair("account", account.to_string(), true));
    }
    if let Some(status) = event.status {
        fields.push(json_pair("status", status.to_string(), false));
    }
    for (key, value) in &event.metadata {
        fields.push(json_pair(key, redact_metadata_value(key, value), true));
    }
    if let Some(payload) = event.payload {
        if config.raw_payloads {
            fields.push(json_pair(
                "raw_payload_base64",
                BASE64_STANDARD.encode(payload),
                true,
            ));
            fields.push(json_pair(
                "raw_payload_bytes",
                payload.len().to_string(),
                false,
            ));
        } else {
            fields.push(json_pair(
                "payload_summary",
                sanitized_payload_summary(payload),
                true,
            ));
            fields.push(json_pair("payload_bytes", payload.len().to_string(), false));
        }
    }
    format!("{{{}}}", fields.join(","))
}

fn sanitized_payload_summary(payload: &[u8]) -> String {
    let preview_len = payload.len().min(MAX_SANITIZED_PAYLOAD_BYTES);
    let preview = String::from_utf8_lossy(&payload[..preview_len]).to_string();
    let preview = redact_sensitive_text(&preview);
    if payload.len() > preview_len {
        format!(
            "{preview}...[truncated {} bytes]",
            payload.len() - preview_len
        )
    } else {
        preview
    }
}

fn redact_metadata_value(key: &str, value: &str) -> String {
    if is_sensitive_name(key) {
        "[redacted]".to_string()
    } else {
        redact_sensitive_text(value)
    }
}

fn redact_sensitive_text(value: &str) -> String {
    let mut redacted = value.to_string();
    for name in [
        "Authorization",
        "Cookie",
        "Set-Cookie",
        "Password",
        "Passcode",
        "Token",
        "AccessToken",
        "RefreshToken",
        "Bearer",
        "Basic",
        "access_token",
        "refresh_token",
        "client_secret",
    ] {
        redacted = redact_named_text(&redacted, name);
    }
    redacted
}

fn redact_named_text(input: &str, name: &str) -> String {
    let lower = input.to_ascii_lowercase();
    let needle = name.to_ascii_lowercase();
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;
    while let Some(relative) = lower[cursor..].find(&needle) {
        let start = cursor + relative;
        output.push_str(&input[cursor..start]);
        output.push_str(&input[start..start + name.len().min(input.len() - start)]);
        let mut end = start + name.len();
        if input.as_bytes().get(end) == Some(&b'>') {
            let close_tag = format!("</{name}>").to_ascii_lowercase();
            if let Some(relative_close) = lower[end + 1..].find(&close_tag) {
                end = end + 1 + relative_close;
            }
        }
        while end < input.len() {
            let byte = input.as_bytes()[end];
            if matches!(byte, b'\n' | b'\r' | b'<' | b'>' | b'&' | b',' | b';') {
                break;
            }
            end += 1;
        }
        output.push_str("=[redacted]");
        cursor = end;
    }
    output.push_str(&input[cursor..]);
    output
}

fn is_sensitive_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.contains("authorization")
        || lower.contains("cookie")
        || lower.contains("password")
        || lower.contains("token")
        || lower.contains("secret")
}

fn json_pair(key: &str, value: String, quote: bool) -> String {
    if quote {
        format!("\"{}\":\"{}\"", escape_json(key), escape_json(&value))
    } else {
        format!("\"{}\":{}", escape_json(key), value)
    }
}

fn escape_json(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                escaped.push_str(&format!("\\u{:04x}", character as u32));
            }
            character => escaped.push(character),
        }
    }
    escaped
}

fn safe_component(component: &str) -> String {
    let safe = component
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '-')
        .map(|character| character.to_ascii_lowercase())
        .collect::<String>();
    if safe.is_empty() {
        "outlook".to_string()
    } else {
        safe
    }
}

fn stable_hash(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn unix_timestamp_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_trace_dir(name: &str) -> PathBuf {
        let path = env::temp_dir().join(format!("lpe-outlook-trace-{name}-{}", Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn disabled_trace_does_not_create_files() {
        let dir = temp_trace_dir("disabled");
        let config = OutlookTraceConfig {
            enabled: false,
            raw_payloads: false,
            directory: dir.clone(),
        };

        write_outlook_trace_with_config(&config, &sample_event("session-1", b"body"));

        assert_eq!(fs::read_dir(dir).unwrap().count(), 0);
    }

    #[test]
    fn trace_file_name_uses_generated_safe_session_hash() {
        let dir = temp_trace_dir("filename");
        let config = OutlookTraceConfig {
            enabled: true,
            raw_payloads: false,
            directory: dir.clone(),
        };

        write_outlook_trace_with_config(
            &config,
            &sample_event("../../tenant@example.test", b"hello"),
        );

        let files = fs::read_dir(dir)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(files.len(), 1);
        let file_name = files[0].file_name().to_string_lossy().to_string();
        assert!(file_name.starts_with("outlook-mapi-"));
        assert!(file_name.ends_with(".jsonl"));
        assert!(!file_name.contains("tenant"));
        assert!(!file_name.contains(".."));
    }

    #[test]
    fn trace_events_append_in_order() {
        let dir = temp_trace_dir("order");
        let config = OutlookTraceConfig {
            enabled: true,
            raw_payloads: false,
            directory: dir.clone(),
        };

        let mut inbound = sample_event("session-2", b"request");
        inbound.direction = OutlookTraceDirection::Inbound;
        inbound.phase = "request";
        write_outlook_trace_with_config(&config, &inbound);
        let mut outbound = sample_event("session-2", b"response");
        outbound.direction = OutlookTraceDirection::Outbound;
        outbound.phase = "response";
        write_outlook_trace_with_config(&config, &outbound);

        let file = fs::read_dir(dir).unwrap().next().unwrap().unwrap().path();
        let content = fs::read_to_string(file).unwrap();
        let request_index = content.find("\"phase\":\"request\"").unwrap();
        let response_index = content.find("\"phase\":\"response\"").unwrap();
        assert!(request_index < response_index);
    }

    #[test]
    fn sanitized_mode_redacts_secrets_without_raw_payload() {
        let dir = temp_trace_dir("redact");
        let config = OutlookTraceConfig {
            enabled: true,
            raw_payloads: false,
            directory: dir.clone(),
        };
        let payload =
            b"Authorization: Basic abc123\r\n<Password>secret</Password><Token>abc</Token>";

        write_outlook_trace_with_config(&config, &sample_event("session-3", payload));

        let file = fs::read_dir(dir).unwrap().next().unwrap().unwrap().path();
        let content = fs::read_to_string(file).unwrap();
        assert!(content.contains("payload_summary"));
        assert!(!content.contains("raw_payload_base64"));
        assert!(!content.contains("abc123"));
        assert!(!content.contains("secret"));
        assert!(!content.contains(">abc<"));
    }

    #[test]
    fn raw_mode_writes_payload_only_when_explicitly_enabled() {
        let dir = temp_trace_dir("raw");
        let config = OutlookTraceConfig {
            enabled: true,
            raw_payloads: true,
            directory: dir.clone(),
        };

        write_outlook_trace_with_config(&config, &sample_event("session-4", b"raw-body"));

        let file = fs::read_dir(dir).unwrap().next().unwrap().unwrap().path();
        let content = fs::read_to_string(file).unwrap();
        assert!(content.contains("raw_payload_base64"));
        assert!(content.contains(&BASE64_STANDARD.encode(b"raw-body")));
        assert!(!content.contains("payload_summary"));
    }

    fn sample_event<'a>(session_key: &'a str, payload: &'a [u8]) -> OutlookTraceEvent<'a> {
        OutlookTraceEvent {
            component: "mapi",
            endpoint: "emsmdb",
            session_key,
            direction: OutlookTraceDirection::Inbound,
            phase: "request",
            remote_peer: Some("203.0.113.7"),
            tenant_id: Some("tenant-1"),
            account: Some("user@example.test"),
            status: None,
            metadata: vec![
                ("authorization", "Basic abc123".to_string()),
                ("request_type", "Connect".to_string()),
            ],
            payload: Some(payload),
        }
    }
}
