use anyhow::Result;
use std::path::Path;

use crate::{
    constants::UNKNOWN_LABELS,
    system::SystemDetector,
    types::{
        DetectionSource, Detector, ExpectedKind, IngressContext, MagikaDetection, PolicyDecision,
        ValidationOutcome, ValidationRequest,
    },
};

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
        | IngressContext::ImapAppend
        | IngressContext::ActiveSyncMimeSubmission
        | IngressContext::SmtpClientSubmission => {
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
