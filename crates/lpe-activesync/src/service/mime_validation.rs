use anyhow::{bail, Result};
use lpe_magika::{
    collect_mime_attachment_parts, Detector, ExpectedKind, IngressContext, PolicyDecision,
    ValidationRequest, Validator,
};
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
    use super::validate_mime_attachments_with_validator;
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
}
