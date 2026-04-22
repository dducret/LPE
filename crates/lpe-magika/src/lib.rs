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
pub use crate::record::{
    read_validation_record, validation_sidecar_path, write_validation_record,
};
pub use crate::system::SystemDetector;
pub use crate::types::{
    DetectionSource, Detector, ExpectedKind, IngressContext, MagikaDetection, MimeAttachmentPart,
    PersistedValidationRecord, PolicyDecision, ValidationOutcome, ValidationRequest,
    VisibleBodyParts,
};
pub use crate::validator::Validator;

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

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
    fn smtp_client_submission_unknown_file_is_restricted() {
        let validator = Validator::new(
            FakeDetector {
                detection: MagikaDetection {
                    label: "unknown_binary".to_string(),
                    mime_type: "application/octet-stream".to_string(),
                    description: "unknown".to_string(),
                    group: "unknown".to_string(),
                    extensions: Vec::new(),
                    score: Some(0.99),
                },
            },
            0.80,
        );
        let outcome = validator
            .validate_bytes(
                ValidationRequest {
                    ingress_context: IngressContext::SmtpClientSubmission,
                    declared_mime: None,
                    filename: None,
                    expected_kind: ExpectedKind::Any,
                },
                b"blob",
            )
            .unwrap();
        assert_eq!(outcome.policy_decision, PolicyDecision::Restrict);
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

    #[test]
    fn extract_visible_text_prefers_plaintext_from_multipart_alternative() {
        let message = concat!(
            "Subject: =?UTF-8?Q?Bonjour_=C3=A9quipe?=\r\n",
            "Content-Type: multipart/alternative; boundary=\"b1\"\r\n",
            "\r\n",
            "--b1\r\n",
            "Content-Type: text/plain; charset=utf-8\r\n",
            "Content-Transfer-Encoding: quoted-printable\r\n",
            "\r\n",
            "Ligne=20un=0ALigne=20deux\r\n",
            "--b1\r\n",
            "Content-Type: text/html; charset=utf-8\r\n",
            "\r\n",
            "<p>Ignored</p>\r\n",
            "--b1--\r\n"
        );

        assert_eq!(
            parse_rfc822_header_value(message.as_bytes(), "subject").as_deref(),
            Some("Bonjour équipe")
        );
        assert_eq!(
            extract_visible_text(message.as_bytes()).unwrap(),
            "Ligne un\nLigne deux"
        );
    }

    #[test]
    fn extract_visible_text_uses_html_when_plaintext_is_missing() {
        let message = concat!(
            "Content-Type: text/html; charset=utf-8\r\n",
            "Content-Transfer-Encoding: base64\r\n",
            "\r\n",
            "PHA+SGVsbG88L3A+\r\n"
        );

        assert_eq!(extract_visible_text(message.as_bytes()).unwrap(), "Hello");
    }

    #[test]
    fn collect_mime_attachment_parts_handles_non_utf8_body_bytes() {
        let mut message = b"Content-Type: multipart/mixed; boundary=\"b1\"\r\n\r\n--b1\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=\"blob.bin\"\r\n\r\n".to_vec();
        message.extend_from_slice(&[0xff, 0xfe, 0x00, 0x41]);
        message.extend_from_slice(b"\r\n--b1--\r\n");

        let attachments = collect_mime_attachment_parts(&message).unwrap();
        assert_eq!(attachments.len(), 1);
        assert_eq!(
            attachments[0].bytes,
            vec![0xff, 0xfe, 0x00, 0x41, b'\r', b'\n']
        );
    }
}
