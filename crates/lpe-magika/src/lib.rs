mod constants;
mod detection;
mod mime;
mod record;
mod system;
mod types;
mod validator;

pub use crate::mime::{
    collect_mime_attachment_parts, extract_visible_body_parts, extract_visible_text,
    parse_rfc822_header_value,
};
pub use crate::record::{read_validation_record, validation_sidecar_path, write_validation_record};
pub use crate::system::SystemDetector;
pub use crate::types::{
    DetectionSource, Detector, ExpectedKind, IngressContext, MagikaDetection, MimeAttachmentPart,
    PersistedValidationRecord, PolicyDecision, ValidationOutcome, ValidationRequest,
    VisibleBodyParts,
};
pub use crate::validator::Validator;

#[cfg(test)]
mod tests;
