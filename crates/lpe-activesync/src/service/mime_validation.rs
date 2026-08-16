use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lpe_magika::{
    collect_mime_attachment_parts, Detector, ExpectedKind, IngressContext, PolicyDecision,
    ValidationRequest, Validator,
};

use crate::wbxml::WbxmlNode;

pub(super) fn validate_contact_picture(application_data: &WbxmlNode) -> Result<()> {
    validate_contact_picture_with_validator(&Validator::from_env(), application_data)
}

pub(super) fn validate_contact_picture_with_validator<D: Detector>(
    validator: &Validator<D>,
    application_data: &WbxmlNode,
) -> Result<()> {
    let Some((data, bytes, content_type)) = decode_contact_picture(application_data)? else {
        return Ok(());
    };
    if data.is_empty() {
        return Ok(());
    }
    let outcome = validator.validate_bytes(
        ValidationRequest {
            ingress_context: IngressContext::ActiveSyncMimeSubmission,
            declared_mime: Some(content_type),
            filename: Some("contact-picture".to_string()),
            expected_kind: ExpectedKind::Any,
        },
        &bytes,
    )?;
    if outcome.policy_decision != PolicyDecision::Accept {
        bail!(
            "ActiveSync contact Picture blocked by Magika validation: {}",
            outcome.reason
        );
    }
    Ok(())
}

pub(super) fn decode_contact_picture(
    application_data: &WbxmlNode,
) -> Result<Option<(String, Vec<u8>, String)>> {
    let Some(node) = application_data.child("Picture") else {
        return Ok(None);
    };
    let data = node.text_value().trim().to_string();
    if data.is_empty() {
        return Ok(Some((data, Vec::new(), String::new())));
    }
    if data.len() > 48 * 1024 {
        bail!("ActiveSync contact Picture exceeds the 48 KB encoded limit");
    }
    let bytes = BASE64
        .decode(&data)
        .map_err(|_| anyhow!("invalid ActiveSync contact Picture base64 data"))?;
    let content_type = if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        "image/jpeg"
    } else if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        "image/gif"
    } else if bytes.starts_with(b"BM") {
        "image/bmp"
    } else {
        "application/octet-stream"
    };
    Ok(Some((data, bytes, content_type.to_string())))
}

pub(super) fn validate_mime_attachments(bytes: &[u8]) -> Result<()> {
    validate_mime_attachments_with_validator(&Validator::from_env(), bytes)
}

pub(super) fn validate_mime_attachments_with_validator<D: Detector>(
    validator: &Validator<D>,
    bytes: &[u8],
) -> Result<()> {
    for attachment in collect_mime_attachment_parts(bytes)? {
        let outcome = validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::ActiveSyncMimeSubmission,
                declared_mime: attachment.declared_mime.clone(),
                filename: attachment.filename.clone(),
                expected_kind: ExpectedKind::Any,
            },
            &attachment.bytes,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            bail!(
                "ActiveSync SendMail blocked by Magika validation for {:?}: {}",
                attachment.filename,
                outcome.reason
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        validate_contact_picture_with_validator, validate_mime_attachments_with_validator,
    };
    use crate::wbxml::WbxmlNode;
    use lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator};

    #[derive(Debug, Clone)]
    struct FakeDetector {
        detection: MagikaDetection,
    }

    impl Detector for FakeDetector {
        fn detect(&self, _source: DetectionSource<'_>) -> anyhow::Result<MagikaDetection> {
            Ok(self.detection.clone())
        }
    }

    #[test]
    fn activesync_sendmail_blocks_mismatched_attachment_payloads() {
        let validator = Validator::new(
            FakeDetector {
                detection: MagikaDetection {
                    label: "exe".to_string(),
                    mime_type: "application/x-msdownload".to_string(),
                    description: "exe".to_string(),
                    group: "binary".to_string(),
                    extensions: vec!["exe".to_string()],
                    score: Some(0.99),
                },
            },
            0.80,
        );
        let mime = concat!(
            "Content-Type: multipart/mixed; boundary=\"abc\"\r\n",
            "\r\n",
            "--abc\r\n",
            "Content-Type: text/plain\r\n",
            "\r\n",
            "Body\r\n",
            "--abc\r\n",
            "Content-Type: application/pdf; name=\"invoice.pdf\"\r\n",
            "Content-Disposition: attachment; filename=\"invoice.pdf\"\r\n",
            "\r\n",
            "%PDF-1.7\r\n",
            "--abc--\r\n"
        );

        let error =
            validate_mime_attachments_with_validator(&validator, mime.as_bytes()).unwrap_err();
        assert!(error.to_string().contains("ActiveSync SendMail blocked"));
    }

    #[test]
    fn activesync_contact_picture_requires_magika_acceptance() {
        let mut application_data = WbxmlNode::new(0, "ApplicationData");
        application_data.push(WbxmlNode::with_text(1, "Picture", "iVBORw0KGgo="));
        let validator = Validator::new(
            FakeDetector {
                detection: MagikaDetection {
                    label: "exe".to_string(),
                    mime_type: "application/x-msdownload".to_string(),
                    description: "exe".to_string(),
                    group: "binary".to_string(),
                    extensions: vec!["exe".to_string()],
                    score: Some(0.99),
                },
            },
            0.80,
        );

        let error =
            validate_contact_picture_with_validator(&validator, &application_data).unwrap_err();
        assert!(error
            .to_string()
            .contains("ActiveSync contact Picture blocked"));
    }
}
