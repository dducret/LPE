use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;

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
    ImapAppend,
    PstUpload,
    PstProcessing,
    AttachmentParsing,
    ActiveSyncMimeSubmission,
    SmtpClientSubmission,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MimeAttachmentPart {
    pub filename: Option<String>,
    pub declared_mime: Option<String>,
    pub content_disposition: Option<String>,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibleBodyParts {
    pub text_body: String,
    pub html_body: Option<String>,
}
