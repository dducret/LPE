use anyhow::{bail, Result};
use lpe_magika::{IngressContext, PolicyDecision, ValidationRequest};

use crate::{upload::expected_attachment_kind, JmapService};

impl<S: crate::store::JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) fn validate_imported_attachments(
        &self,
        attachments: &[lpe_storage::AttachmentUploadInput],
    ) -> Result<()> {
        for attachment in attachments {
            let outcome = self.validator.validate_bytes(
                ValidationRequest {
                    ingress_context: IngressContext::AttachmentParsing,
                    declared_mime: Some(attachment.media_type.clone()),
                    filename: Some(attachment.file_name.clone()),
                    expected_kind: expected_attachment_kind(
                        attachment.media_type.as_str(),
                        attachment.file_name.as_str(),
                    ),
                },
                &attachment.blob_bytes,
            )?;
            if outcome.policy_decision != PolicyDecision::Accept {
                bail!(
                    "JMAP email import attachment '{}' blocked by Magika validation: {}",
                    attachment.file_name,
                    outcome.reason
                );
            }
        }

        Ok(())
    }
}
