use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use std::{
    collections::hash_map::DefaultHasher,
    env,
    fs::{self, OpenOptions},
    hash::{Hash, Hasher},
    io::{BufRead, BufReader, Write},
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
    let paths = trace_file_paths(&config.directory, event.component, event.session_key);
    let sequence = next_trace_sequence(&paths.legacy)?;
    let context = TraceRenderContext::new(event.session_key, sequence);
    let path = paths.legacy;
    let mut file = open_trace_file(&path)?;
    file.write_all(render_event(config, event, &context).as_bytes())?;
    file.write_all(b"\n")?;
    if !is_protocol_event(event) {
        let mut diagnostics = open_trace_file(&paths.diagnostics)?;
        diagnostics.write_all(render_event(config, event, &context).as_bytes())?;
        diagnostics.write_all(b"\n")?;
        return Ok(());
    }
    let mut rr = open_trace_file(&paths.rr)?;
    rr.write_all(render_request_response_event(config, event, &context).as_bytes())?;
    rr.write_all(b"\n")?;
    let mut replay = open_trace_file(&paths.replay)?;
    replay.write_all(render_replay_event(event, &context).as_bytes())?;
    replay.write_all(b"\n")?;
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct TraceFilePaths {
    legacy: PathBuf,
    diagnostics: PathBuf,
    replay: PathBuf,
    rr: PathBuf,
}

fn trace_file_paths(directory: &Path, component: &str, session_key: &str) -> TraceFilePaths {
    let stem = format!(
        "outlook-{}-{:016x}",
        safe_component(component),
        stable_hash(session_key)
    );
    TraceFilePaths {
        legacy: directory.join(format!("{stem}.jsonl")),
        diagnostics: directory.join(format!("{stem}.diagnostics.jsonl")),
        replay: directory.join(format!("{stem}.replay.jsonl")),
        rr: directory.join(format!("{stem}.rr.jsonl")),
    }
}

fn next_trace_sequence(path: &Path) -> std::io::Result<u64> {
    match std::fs::File::open(path) {
        Ok(file) => Ok(BufReader::new(file).lines().count() as u64 + 1),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(1),
        Err(error) => Err(error),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TraceRenderContext {
    timestamp_unix_ms: u128,
    event_id: String,
    session_id: String,
    sequence: u64,
    step_id: String,
}

impl TraceRenderContext {
    fn new(session_key: &str, sequence: u64) -> Self {
        let session_id = format!("{:016x}", stable_hash(session_key));
        Self {
            timestamp_unix_ms: unix_timestamp_millis(),
            event_id: Uuid::new_v4().to_string(),
            step_id: format!("{session_id}-{sequence:06}"),
            session_id,
            sequence,
        }
    }
}

fn render_event(
    config: &OutlookTraceConfig,
    event: &OutlookTraceEvent<'_>,
    context: &TraceRenderContext,
) -> String {
    let mut fields = vec![
        json_pair(
            "timestamp_unix_ms",
            context.timestamp_unix_ms.to_string(),
            false,
        ),
        json_pair("event_id", context.event_id.clone(), true),
        json_pair("step_id", context.step_id.clone(), true),
        json_pair("sequence", context.sequence.to_string(), false),
        json_pair("component", event.component.to_string(), true),
        json_pair("endpoint", event.endpoint.to_string(), true),
        json_pair("session_id", context.session_id.clone(), true),
        json_pair("direction", event.direction.as_str().to_string(), true),
        json_pair("phase", event.phase.to_string(), true),
    ];
    if !is_protocol_event(event) {
        fields.push(json_pair("protocol_event", "false".to_string(), false));
    }
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
    for (key, value) in event
        .metadata
        .iter()
        .filter(|(key, _)| !key.eq_ignore_ascii_case("protocol_event"))
    {
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

fn render_replay_event(event: &OutlookTraceEvent<'_>, context: &TraceRenderContext) -> String {
    let mut fields = vec![
        json_pair("step_id", context.step_id.clone(), true),
        json_pair("sequence", context.sequence.to_string(), false),
        json_pair("session_id", context.session_id.clone(), true),
        json_pair("component", event.component.to_string(), true),
        json_pair("endpoint", event.endpoint.to_string(), true),
        json_pair("direction", event.direction.as_str().to_string(), true),
        json_pair("phase", event.phase.to_string(), true),
    ];
    if let Some(tenant_id) = event.tenant_id {
        fields.push(json_pair("tenant_id", tenant_id.to_string(), true));
    }
    if let Some(account) = event.account {
        fields.push(json_pair("account", account.to_string(), true));
    }
    if let Some(status) = event.status {
        fields.push(json_pair("status", status.to_string(), false));
    }
    fields.push(json_object_pair("metadata", &redacted_metadata(event)));
    if let Some(payload) = event.payload {
        fields.push(json_pair("payload_bytes", payload.len().to_string(), false));
    }
    format!("{{{}}}", fields.join(","))
}

fn render_request_response_event(
    config: &OutlookTraceConfig,
    event: &OutlookTraceEvent<'_>,
    context: &TraceRenderContext,
) -> String {
    let mut fields = vec![
        json_pair(
            "timestamp_unix_ms",
            context.timestamp_unix_ms.to_string(),
            false,
        ),
        json_pair("event_id", context.event_id.clone(), true),
        json_pair("step_id", context.step_id.clone(), true),
        json_pair("sequence", context.sequence.to_string(), false),
        json_pair("session_id", context.session_id.clone(), true),
        json_pair("component", event.component.to_string(), true),
        json_pair("protocol", event.component.to_string(), true),
        json_pair("endpoint", event.endpoint.to_string(), true),
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
        fields.push(json_pair("response_status", status.to_string(), false));
    }
    fields.push(json_object_pair("metadata", &redacted_metadata(event)));
    if let Some(payload) = event.payload {
        let payload_field = match event.direction {
            OutlookTraceDirection::Inbound => "request_body",
            OutlookTraceDirection::Outbound => "response_body",
        };
        if config.raw_payloads {
            fields.push(json_pair(
                &format!("{payload_field}_base64"),
                BASE64_STANDARD.encode(payload),
                true,
            ));
        } else {
            fields.push(json_pair(
                &format!("{payload_field}_summary"),
                sanitized_payload_summary(payload),
                true,
            ));
        }
        fields.push(json_pair(
            &format!("{payload_field}_bytes"),
            payload.len().to_string(),
            false,
        ));
    }
    format!("{{{}}}", fields.join(","))
}

fn redacted_metadata(event: &OutlookTraceEvent<'_>) -> Vec<(String, String)> {
    event
        .metadata
        .iter()
        .map(|(key, value)| (key.to_string(), redact_metadata_value(key, value)))
        .collect()
}

fn is_protocol_event(event: &OutlookTraceEvent<'_>) -> bool {
    !event.metadata.iter().any(|(key, value)| {
        key.eq_ignore_ascii_case("protocol_event") && value.trim().eq_ignore_ascii_case("false")
    })
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

fn json_object_pair(key: &str, values: &[(String, String)]) -> String {
    let fields = values
        .iter()
        .map(|(field_key, field_value)| {
            format!(
                "\"{}\":\"{}\"",
                escape_json(field_key),
                escape_json(field_value)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("\"{}\":{{{}}}", escape_json(key), fields)
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
    use std::{collections::HashMap, fs};

    fn temp_trace_dir(name: &str) -> PathBuf {
        let path = env::temp_dir().join(format!("lpe-outlook-trace-{name}-{}", Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn trace_file_names(dir: &Path) -> Vec<String> {
        let mut files = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        files.sort();
        files
    }

    fn trace_file_with_suffix(dir: &Path, suffix: &str) -> PathBuf {
        fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .ends_with(suffix)
            })
            .unwrap()
    }

    fn legacy_trace_file(dir: &Path) -> PathBuf {
        fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                let name = path.file_name().unwrap().to_string_lossy();
                name.ends_with(".jsonl")
                    && !name.ends_with(".diagnostics.jsonl")
                    && !name.ends_with(".replay.jsonl")
                    && !name.ends_with(".rr.jsonl")
            })
            .unwrap()
    }

    fn json_string_value(line: &str, key: &str) -> String {
        let needle = format!("\"{key}\":\"");
        let start = line.find(&needle).unwrap() + needle.len();
        let end = line[start..].find('"').unwrap() + start;
        line[start..end].to_string()
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

        let files = trace_file_names(&dir);
        assert_eq!(files.len(), 3);
        assert!(files.iter().any(|file| file.ends_with(".jsonl")));
        assert!(files.iter().any(|file| file.ends_with(".replay.jsonl")));
        assert!(files.iter().any(|file| file.ends_with(".rr.jsonl")));
        for file_name in files {
            assert!(file_name.starts_with("outlook-mapi-"));
            assert!(file_name.ends_with(".jsonl"));
            assert!(!file_name.contains("tenant"));
            assert!(!file_name.contains(".."));
        }
    }

    #[test]
    fn trace_events_append_in_order_with_matching_replay_and_rr_steps() {
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

        let content = fs::read_to_string(legacy_trace_file(&dir)).unwrap();
        let request_index = content.find("\"phase\":\"request\"").unwrap();
        let response_index = content.find("\"phase\":\"response\"").unwrap();
        assert!(request_index < response_index);
        assert!(content.contains("\"sequence\":1"));
        assert!(content.contains("\"sequence\":2"));

        let replay = fs::read_to_string(trace_file_with_suffix(&dir, ".replay.jsonl")).unwrap();
        let rr = fs::read_to_string(trace_file_with_suffix(&dir, ".rr.jsonl")).unwrap();
        let replay_lines = replay.lines().collect::<Vec<_>>();
        let rr_lines = rr.lines().collect::<Vec<_>>();
        assert_eq!(replay_lines.len(), 2);
        assert_eq!(rr_lines.len(), 2);
        assert!(replay_lines[0].contains("\"phase\":\"request\""));
        assert!(replay_lines[1].contains("\"phase\":\"response\""));
        for (replay_line, rr_line) in replay_lines.iter().zip(rr_lines.iter()) {
            assert_eq!(
                json_string_value(replay_line, "step_id"),
                json_string_value(rr_line, "step_id")
            );
        }
    }

    #[test]
    fn mapi_protocol_exports_ignore_non_protocol_diagnostics_for_request_pairing() {
        let dir = temp_trace_dir("mapi-pairing");
        let config = OutlookTraceConfig {
            enabled: true,
            raw_payloads: false,
            directory: dir.clone(),
        };

        let mut diagnostic = sample_event("session-2b", b"");
        diagnostic.direction = OutlookTraceDirection::Outbound;
        diagnostic.phase = "ExecutePostCommonViewsHandoff";
        diagnostic.status = Some(200);
        diagnostic.metadata = vec![
            ("protocol_event", "false".to_string()),
            ("diagnostic_stream", "post_common_views_handoff".to_string()),
            ("mapi_request_id", "req-1".to_string()),
        ];
        diagnostic.payload = None;
        write_outlook_trace_with_config(&config, &diagnostic);

        let mut inbound = sample_event("session-2b", b"execute-request");
        inbound.direction = OutlookTraceDirection::Inbound;
        inbound.phase = "Execute";
        inbound.metadata = vec![("mapi_request_id", "req-1".to_string())];
        write_outlook_trace_with_config(&config, &inbound);
        let mut outbound = sample_event("session-2b", b"execute-response");
        outbound.direction = OutlookTraceDirection::Outbound;
        outbound.phase = "Execute";
        outbound.status = Some(200);
        outbound.metadata = vec![("mapi_request_id", "req-1".to_string())];
        write_outlook_trace_with_config(&config, &outbound);

        let diagnostics =
            fs::read_to_string(trace_file_with_suffix(&dir, ".diagnostics.jsonl")).unwrap();
        assert!(diagnostics.contains("\"phase\":\"ExecutePostCommonViewsHandoff\""));
        assert!(diagnostics.contains("\"protocol_event\":false"));

        let replay = fs::read_to_string(trace_file_with_suffix(&dir, ".replay.jsonl")).unwrap();
        let rr = fs::read_to_string(trace_file_with_suffix(&dir, ".rr.jsonl")).unwrap();
        let replay_lines = replay.lines().collect::<Vec<_>>();
        let rr_lines = rr.lines().collect::<Vec<_>>();
        assert_eq!(replay_lines.len(), 2);
        assert_eq!(rr_lines.len(), 2);
        validate_mapi_protocol_request_response_pairs(&replay_lines);
        validate_mapi_protocol_request_response_pairs(&rr_lines);
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

        let content = fs::read_to_string(legacy_trace_file(&dir)).unwrap();
        assert!(content.contains("payload_summary"));
        assert!(!content.contains("raw_payload_base64"));
        assert!(!content.contains("abc123"));
        assert!(!content.contains("secret"));
        assert!(!content.contains(">abc<"));
        let rr = fs::read_to_string(trace_file_with_suffix(&dir, ".rr.jsonl")).unwrap();
        assert!(rr.contains("request_body_summary"));
        assert!(!rr.contains("abc123"));
        assert!(!rr.contains("secret"));
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

        let content = fs::read_to_string(legacy_trace_file(&dir)).unwrap();
        assert!(content.contains("raw_payload_base64"));
        assert!(content.contains(&BASE64_STANDARD.encode(b"raw-body")));
        assert!(!content.contains("payload_summary"));
        let rr = fs::read_to_string(trace_file_with_suffix(&dir, ".rr.jsonl")).unwrap();
        assert!(rr.contains("request_body_base64"));
        assert!(rr.contains(&BASE64_STANDARD.encode(b"raw-body")));
    }

    #[test]
    fn rr_trace_names_outbound_payload_as_response_body() {
        let dir = temp_trace_dir("outbound-raw");
        let config = OutlookTraceConfig {
            enabled: true,
            raw_payloads: true,
            directory: dir.clone(),
        };
        let mut event = sample_event("session-5", b"response-body");
        event.direction = OutlookTraceDirection::Outbound;

        write_outlook_trace_with_config(&config, &event);

        let rr = fs::read_to_string(trace_file_with_suffix(&dir, ".rr.jsonl")).unwrap();
        assert!(rr.contains("response_body_base64"));
        assert!(rr.contains(&BASE64_STANDARD.encode(b"response-body")));
        assert!(!rr.contains("request_body_base64"));
    }

    fn validate_mapi_protocol_request_response_pairs(lines: &[&str]) {
        let mut open_requests: HashMap<String, usize> = HashMap::new();
        let mut inbound_count: HashMap<String, usize> = HashMap::new();
        let mut outbound_count: HashMap<String, usize> = HashMap::new();
        for line in lines
            .iter()
            .copied()
            .filter(|line| line.contains("\"component\":\"mapi\""))
        {
            let request_id = json_string_value(line, "mapi_request_id");
            match json_string_value(line, "direction").as_str() {
                "inbound" => {
                    *open_requests.entry(request_id.clone()).or_default() += 1;
                    *inbound_count.entry(request_id).or_default() += 1;
                }
                "outbound" => {
                    let pending = open_requests
                        .get_mut(&request_id)
                        .expect("outbound MAPI response without preceding request");
                    assert!(
                        *pending > 0,
                        "outbound MAPI response without preceding request"
                    );
                    *pending -= 1;
                    *outbound_count.entry(request_id).or_default() += 1;
                }
                direction => panic!("unexpected MAPI trace direction {direction}"),
            }
        }

        assert!(
            open_requests.values().all(|pending| *pending == 0),
            "every inbound MAPI request must have a protocol response"
        );
        assert_eq!(
            inbound_count, outbound_count,
            "each inbound MAPI request must have exactly one protocol response"
        );
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
