use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

const DEFAULT_MAGIKA_MIN_SCORE: f32 = 0.80;
const UNKNOWN_LABELS: &[&str] = &[
    "unknown",
    "unknown_binary",
    "unknown_text",
    "generic_text",
    "generic_binary",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyDecision {
    Accept,
    Restrict,
    Quarantine,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IngressContext {
    JmapUpload,
    JmapEmailImport,
    PstUpload,
    PstProcessing,
    AttachmentParsing,
    ActiveSyncMimeSubmission,
    LpeCtInboundSmtp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpectedKind {
    Any,
    Rfc822Message,
    Pst,
    SupportedAttachmentText,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationRequest {
    pub ingress_context: IngressContext,
    pub declared_mime: Option<String>,
    pub filename: Option<String>,
    pub expected_kind: ExpectedKind,
}

impl ValidationRequest {
    pub fn new(ingress_context: IngressContext) -> Self {
        Self {
            ingress_context,
            declared_mime: None,
            filename: None,
            expected_kind: ExpectedKind::Any,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationOutcome {
    pub detected_label: String,
    pub detected_mime: String,
    pub description: String,
    pub group: String,
    pub extensions: Vec<String>,
    pub score: Option<f32>,
    pub declared_mime: Option<String>,
    pub filename: Option<String>,
    pub mismatch: bool,
    pub policy_decision: PolicyDecision,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedValidationRecord {
    pub version: u32,
    pub created_at: String,
    pub ingress_context: IngressContext,
    pub file_size: u64,
    pub policy_decision: PolicyDecision,
    pub expected_kind: ExpectedKind,
    pub outcome: ValidationOutcome,
}

#[derive(Debug, Clone)]
pub enum DetectionSource<'a> {
    Bytes(&'a [u8]),
    Path(&'a Path),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MagikaDetection {
    pub label: String,
    pub mime_type: String,
    pub description: String,
    pub group: String,
    pub extensions: Vec<String>,
    pub score: Option<f32>,
}

pub trait Detector: Clone + Send + Sync + 'static {
    fn detect(&self, source: DetectionSource<'_>) -> Result<MagikaDetection>;
}

#[derive(Debug, Clone)]
pub struct SystemDetector {
    command: PathBuf,
    min_score: f32,
}

impl SystemDetector {
    pub fn from_env() -> Self {
        let command = env::var("LPE_MAGIKA_BIN").unwrap_or_else(|_| "magika".to_string());
        let min_score = env::var("LPE_MAGIKA_MIN_SCORE")
            .ok()
            .and_then(|value| value.parse::<f32>().ok())
            .unwrap_or(DEFAULT_MAGIKA_MIN_SCORE);
        Self {
            command: PathBuf::from(command),
            min_score,
        }
    }

    pub fn min_score(&self) -> f32 {
        self.min_score
    }

    fn run_magika(&self, source: DetectionSource<'_>) -> Result<Value> {
        let mut command = Command::new(&self.command);
        command.arg("--json");
        match source {
            DetectionSource::Bytes(bytes) => {
                command.arg("-");
                command.stdin(Stdio::piped());
                command.stdout(Stdio::piped());
                let mut child = command
                    .spawn()
                    .with_context(|| format!("spawn Magika command {}", self.command.display()))?;
                {
                    let stdin = child
                        .stdin
                        .as_mut()
                        .ok_or_else(|| anyhow!("Magika stdin is unavailable"))?;
                    use std::io::Write;
                    stdin.write_all(bytes)?;
                }
                let output = child.wait_with_output()?;
                if !output.status.success() {
                    bail!(
                        "Magika command failed with status {}: {}",
                        output.status,
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
                serde_json::from_slice(&output.stdout).context("parse Magika JSON output")
            }
            DetectionSource::Path(path) => {
                command.arg(path);
                let output = command
                    .output()
                    .with_context(|| format!("run Magika on {}", path.display()))?;
                if !output.status.success() {
                    bail!(
                        "Magika command failed with status {}: {}",
                        output.status,
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
                serde_json::from_slice(&output.stdout).context("parse Magika JSON output")
            }
        }
    }
}

impl Detector for SystemDetector {
    fn detect(&self, source: DetectionSource<'_>) -> Result<MagikaDetection> {
        let raw = self.run_magika(source)?;
        parse_detection_json(raw)
    }
}

#[derive(Debug, Clone)]
pub struct Validator<D> {
    detector: D,
    min_score: f32,
}

impl Validator<SystemDetector> {
    pub fn from_env() -> Self {
        let detector = SystemDetector::from_env();
        let min_score = detector.min_score();
        Self {
            detector,
            min_score,
        }
    }
}

impl<D: Detector> Validator<D> {
    pub fn new(detector: D, min_score: f32) -> Self {
        Self {
            detector,
            min_score,
        }
    }

    pub fn validate(
        &self,
        request: ValidationRequest,
        source: DetectionSource<'_>,
    ) -> Result<ValidationOutcome> {
        let detection = self.detector.detect(source)?;
        Ok(decide_policy(&request, &detection, self.min_score))
    }

    pub fn validate_path(
        &self,
        request: ValidationRequest,
        path: &Path,
    ) -> Result<ValidationOutcome> {
        self.validate(request, DetectionSource::Path(path))
    }

    pub fn validate_bytes(
        &self,
        request: ValidationRequest,
        bytes: &[u8],
    ) -> Result<ValidationOutcome> {
        self.validate(request, DetectionSource::Bytes(bytes))
    }
}

pub fn write_validation_record(
    path: &Path,
    request: &ValidationRequest,
    outcome: &ValidationOutcome,
    file_size: u64,
) -> Result<PathBuf> {
    let record = PersistedValidationRecord {
        version: 1,
        created_at: unix_timestamp(),
        ingress_context: request.ingress_context,
        file_size,
        policy_decision: outcome.policy_decision,
        expected_kind: request.expected_kind,
        outcome: outcome.clone(),
    };
    let sidecar = validation_sidecar_path(path);
    fs::write(&sidecar, serde_json::to_vec_pretty(&record)?)
        .with_context(|| format!("write validation sidecar {}", sidecar.display()))?;
    Ok(sidecar)
}

pub fn read_validation_record(path: &Path) -> Result<PersistedValidationRecord> {
    let sidecar = validation_sidecar_path(path);
    let bytes = fs::read(&sidecar)
        .with_context(|| format!("read validation sidecar {}", sidecar.display()))?;
    serde_json::from_slice(&bytes).context("parse validation sidecar JSON")
}

pub fn validation_sidecar_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".magika.json");
    PathBuf::from(value)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MimeAttachmentPart {
    pub filename: Option<String>,
    pub declared_mime: Option<String>,
    pub content_disposition: Option<String>,
    pub bytes: Vec<u8>,
}

pub fn collect_mime_attachment_parts(bytes: &[u8]) -> Result<Vec<MimeAttachmentPart>> {
    let mut attachments = Vec::new();
    collect_attachment_parts(bytes, &mut attachments)?;
    Ok(attachments)
}

fn collect_attachment_parts(bytes: &[u8], attachments: &mut Vec<MimeAttachmentPart>) -> Result<()> {
    let raw = String::from_utf8_lossy(bytes);
    let (header_block, body_block) = split_headers_and_body(raw.as_ref());
    let headers = parse_rfc822_headers(header_block);
    let content_type = headers
        .get("content-type")
        .cloned()
        .unwrap_or_else(|| "text/plain".to_string());
    let transfer_encoding = headers
        .get("content-transfer-encoding")
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    let decoded_body = decode_transfer_encoding(body_block.as_bytes(), &transfer_encoding)?;

    if content_type.to_ascii_lowercase().starts_with("multipart/") {
        let Some(boundary) = content_type_parameter(&content_type, "boundary") else {
            return Ok(());
        };
        let boundary_marker = format!("--{boundary}");
        let raw_body = String::from_utf8_lossy(&decoded_body);
        for part in raw_body.split(&boundary_marker).skip(1) {
            let trimmed = part.trim();
            if trimmed.is_empty() || trimmed == "--" {
                continue;
            }
            let trimmed = trimmed.trim_start_matches("\r\n").trim_start_matches('\n');
            let trimmed = trimmed.trim_end_matches("--").trim();
            collect_attachment_parts(trimmed.as_bytes(), attachments)?;
        }
        return Ok(());
    }

    let content_disposition = headers.get("content-disposition").cloned();
    let filename = content_disposition
        .as_deref()
        .and_then(|value| content_type_parameter(value, "filename"))
        .or_else(|| content_type_parameter(&content_type, "name"));
    let is_attachment = content_disposition
        .as_deref()
        .map(|value| value.to_ascii_lowercase().starts_with("attachment"))
        .unwrap_or(false);
    if is_attachment || filename.is_some() {
        attachments.push(MimeAttachmentPart {
            filename,
            declared_mime: Some(strip_content_type_parameters(&content_type)),
            content_disposition,
            bytes: decoded_body,
        });
    }
    Ok(())
}

fn parse_detection_json(raw: Value) -> Result<MagikaDetection> {
    let entry = raw
        .as_array()
        .and_then(|entries| entries.first())
        .ok_or_else(|| anyhow!("Magika JSON output is not a non-empty array"))?;
    let result = entry
        .get("result")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("Magika JSON output is missing result"))?;
    let status = result
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if status != "ok" {
        bail!("Magika returned non-ok status: {status}");
    }
    let value = result
        .get("value")
        .ok_or_else(|| anyhow!("Magika JSON output is missing result value"))?;
    let output = value
        .get("output")
        .and_then(Value::as_object)
        .or_else(|| value.as_object())
        .ok_or_else(|| anyhow!("Magika JSON output is missing output"))?;
    let label = output
        .get("label")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let mime_type = output
        .get("mime_type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let description = output
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let group = output
        .get("group")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let extensions = output
        .get("extensions")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(|value| value.to_ascii_lowercase())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let score = value
        .get("score")
        .and_then(Value::as_f64)
        .map(|value| value as f32);

    if label.trim().is_empty() || mime_type.trim().is_empty() {
        bail!("Magika returned an incomplete detection result");
    }

    Ok(MagikaDetection {
        label,
        mime_type,
        description,
        group,
        extensions,
        score,
    })
}

fn decide_policy(
    request: &ValidationRequest,
    detection: &MagikaDetection,
    min_score: f32,
) -> ValidationOutcome {
    let declared_mime = request
        .declared_mime
        .as_ref()
        .map(|value| normalize_mime(value));
    let filename_extension = request.filename.as_deref().and_then(file_extension);
    let detected_mime = normalize_mime(&detection.mime_type);
    let extension_mismatch = filename_extension
        .as_deref()
        .map(|extension| !detection.extensions.iter().any(|known| known == extension))
        .unwrap_or(false);
    let mime_mismatch = declared_mime
        .as_deref()
        .map(|declared| !mime_matches(declared, &detected_mime))
        .unwrap_or(false);
    let expected_mismatch = !matches_expected_kind(request.expected_kind, detection);
    let mismatch = extension_mismatch || mime_mismatch || expected_mismatch;
    let unknown = UNKNOWN_LABELS.contains(&detection.label.as_str()) || detected_mime.is_empty();
    let low_confidence = detection
        .score
        .map(|value| value < min_score)
        .unwrap_or(false);

    let (policy_decision, reason) = match request.ingress_context {
        IngressContext::LpeCtInboundSmtp => {
            if mismatch {
                (
                    PolicyDecision::Reject,
                    "detected content does not match declared attachment type",
                )
            } else if low_confidence {
                (
                    PolicyDecision::Quarantine,
                    "Magika score is below the configured confidence threshold",
                )
            } else if unknown {
                (
                    PolicyDecision::Quarantine,
                    "Magika could not classify the attachment safely",
                )
            } else {
                (PolicyDecision::Accept, "attachment validated")
            }
        }
        IngressContext::AttachmentParsing
        | IngressContext::PstUpload
        | IngressContext::PstProcessing => {
            if mismatch {
                (
                    PolicyDecision::Reject,
                    "detected content does not match the required file type",
                )
            } else if low_confidence {
                (
                    PolicyDecision::Reject,
                    "Magika score is below the configured confidence threshold",
                )
            } else if unknown {
                (
                    PolicyDecision::Reject,
                    "Magika could not classify the file safely",
                )
            } else {
                (PolicyDecision::Accept, "file validated")
            }
        }
        IngressContext::JmapUpload
        | IngressContext::JmapEmailImport
        | IngressContext::ActiveSyncMimeSubmission => {
            if mismatch {
                (
                    PolicyDecision::Reject,
                    "detected content does not match the declared file type",
                )
            } else if low_confidence {
                (
                    PolicyDecision::Reject,
                    "Magika score is below the configured confidence threshold",
                )
            } else if unknown {
                (
                    PolicyDecision::Restrict,
                    "Magika could not classify the file safely",
                )
            } else {
                (PolicyDecision::Accept, "file validated")
            }
        }
    };

    ValidationOutcome {
        detected_label: detection.label.clone(),
        detected_mime,
        description: detection.description.clone(),
        group: detection.group.clone(),
        extensions: detection.extensions.clone(),
        score: detection.score,
        declared_mime,
        filename: request.filename.clone(),
        mismatch,
        policy_decision,
        reason: reason.to_string(),
    }
}

fn matches_expected_kind(expected_kind: ExpectedKind, detection: &MagikaDetection) -> bool {
    match expected_kind {
        ExpectedKind::Any => true,
        ExpectedKind::Rfc822Message => {
            mime_matches(&normalize_mime(&detection.mime_type), "message/rfc822")
        }
        ExpectedKind::Pst => {
            let mime = normalize_mime(&detection.mime_type);
            mime == "application/vnd.ms-outlook"
                || mime == "application/x-hoard-pst"
                || detection.label.eq_ignore_ascii_case("pst")
                || detection
                    .extensions
                    .iter()
                    .any(|extension| extension == "pst")
        }
        ExpectedKind::SupportedAttachmentText => {
            let mime = normalize_mime(&detection.mime_type);
            matches!(
                mime.as_str(),
                "application/pdf"
                    | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    | "application/vnd.oasis.opendocument.text"
            )
        }
    }
}

fn mime_matches(left: &str, right: &str) -> bool {
    let left = normalize_mime(left);
    let right = normalize_mime(right);
    if left == right {
        return true;
    }

    matches!(
        (left.as_str(), right.as_str()),
        ("application/x-pdf", "application/pdf")
            | ("application/pdf", "application/x-pdf")
            | (
                "application/x-msdownload",
                "application/vnd.microsoft.portable-executable"
            )
            | (
                "application/vnd.microsoft.portable-executable",
                "application/x-msdownload"
            )
    )
}

fn normalize_mime(value: &str) -> String {
    strip_content_type_parameters(value).to_ascii_lowercase()
}

fn strip_content_type_parameters(value: &str) -> String {
    value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn file_extension(filename: &str) -> Option<String> {
    Path::new(filename)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
}

fn split_headers_and_body(raw: &str) -> (&str, &str) {
    raw.split_once("\r\n\r\n")
        .or_else(|| raw.split_once("\n\n"))
        .unwrap_or((raw, ""))
}

fn parse_rfc822_headers(block: &str) -> HashMap<String, String> {
    let mut headers: HashMap<String, String> = HashMap::new();
    let mut current_name = String::new();
    for line in block.lines() {
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some(value) = headers.get_mut(&current_name) {
                value.push(' ');
                value.push_str(line.trim());
            }
            continue;
        }
        let Some((name, value)) = line.trim_end_matches('\r').split_once(':') else {
            continue;
        };
        current_name = name.trim().to_ascii_lowercase();
        headers.insert(current_name.clone(), value.trim().to_string());
    }
    headers
}

fn content_type_parameter(header_value: &str, parameter: &str) -> Option<String> {
    for segment in header_value.split(';').skip(1) {
        let (name, value) = segment.split_once('=')?;
        if name.trim().eq_ignore_ascii_case(parameter) {
            return Some(value.trim().trim_matches('"').to_string());
        }
    }
    None
}

fn decode_transfer_encoding(body: &[u8], encoding: &str) -> Result<Vec<u8>> {
    match encoding.trim() {
        "base64" => {
            let compact = String::from_utf8_lossy(body)
                .lines()
                .map(str::trim)
                .collect::<String>();
            Ok(BASE64.decode(compact)?)
        }
        "quoted-printable" => decode_quoted_printable(body),
        _ => Ok(body.to_vec()),
    }
}

fn decode_quoted_printable(body: &[u8]) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(body.len());
    let mut cursor = 0usize;
    while cursor < body.len() {
        match body[cursor] {
            b'=' => {
                if cursor + 1 < body.len()
                    && (body[cursor + 1] == b'\n' || body[cursor + 1] == b'\r')
                {
                    cursor += 1;
                    while cursor < body.len() && (body[cursor] == b'\n' || body[cursor] == b'\r') {
                        cursor += 1;
                    }
                    continue;
                }
                let hex = body
                    .get(cursor + 1..cursor + 3)
                    .ok_or_else(|| anyhow!("invalid quoted-printable sequence"))?;
                let value = std::str::from_utf8(hex)?;
                output.push(u8::from_str_radix(value, 16)?);
                cursor += 3;
            }
            byte => {
                output.push(byte);
                cursor += 1;
            }
        }
    }
    Ok(output)
}

fn unix_timestamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => format!("unix:{}", duration.as_secs()),
        Err(_) => "unix:0".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct FakeDetector {
        detection: MagikaDetection,
    }

    impl Detector for FakeDetector {
        fn detect(&self, _source: DetectionSource<'_>) -> Result<MagikaDetection> {
            Ok(self.detection.clone())
        }
    }

    fn detection(mime_type: &str, extension: &str, score: f32) -> MagikaDetection {
        MagikaDetection {
            label: extension.to_string(),
            mime_type: mime_type.to_string(),
            description: extension.to_string(),
            group: "document".to_string(),
            extensions: vec![extension.to_string()],
            score: Some(score),
        }
    }

    #[test]
    fn supported_attachment_kind_is_accepted() {
        let validator = Validator::new(
            FakeDetector {
                detection: detection("application/pdf", "pdf", 0.99),
            },
            0.80,
        );
        let outcome = validator
            .validate_bytes(
                ValidationRequest {
                    ingress_context: IngressContext::AttachmentParsing,
                    declared_mime: Some("application/pdf".to_string()),
                    filename: Some("report.pdf".to_string()),
                    expected_kind: ExpectedKind::SupportedAttachmentText,
                },
                b"pdf",
            )
            .unwrap();
        assert_eq!(outcome.policy_decision, PolicyDecision::Accept);
    }

    #[test]
    fn smtp_mismatch_is_rejected() {
        let validator = Validator::new(
            FakeDetector {
                detection: detection("application/x-msdownload", "exe", 0.99),
            },
            0.80,
        );
        let outcome = validator
            .validate_bytes(
                ValidationRequest {
                    ingress_context: IngressContext::LpeCtInboundSmtp,
                    declared_mime: Some("application/pdf".to_string()),
                    filename: Some("invoice.pdf".to_string()),
                    expected_kind: ExpectedKind::Any,
                },
                b"exe",
            )
            .unwrap();
        assert_eq!(outcome.policy_decision, PolicyDecision::Reject);
        assert!(outcome.mismatch);
    }

    #[test]
    fn collect_mime_attachment_parts_extracts_attachment_payloads() {
        let message = concat!(
            "Content-Type: multipart/mixed; boundary=\"abc\"\r\n",
            "\r\n",
            "--abc\r\n",
            "Content-Type: text/plain\r\n",
            "\r\n",
            "Body\r\n",
            "--abc\r\n",
            "Content-Type: application/pdf; name=\"report.pdf\"\r\n",
            "Content-Disposition: attachment; filename=\"report.pdf\"\r\n",
            "Content-Transfer-Encoding: base64\r\n",
            "\r\n",
            "UERG\r\n",
            "--abc--\r\n"
        );
        let attachments = collect_mime_attachment_parts(message.as_bytes()).unwrap();
        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].filename.as_deref(), Some("report.pdf"));
        assert_eq!(
            attachments[0].declared_mime.as_deref(),
            Some("application/pdf")
        );
        assert_eq!(attachments[0].bytes, b"PDF".to_vec());
    }
}
