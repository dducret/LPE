---
type: Rust Method
title: validate_imported_attachments
resource: crates/lpe-jmap/src/mail/import_validation.rs#L7-L34
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import
---

# Signature

`pub(crate) fn validate_imported_attachments( &self, attachments: &[lpe_storage::AttachmentUploadInput], ) -> Result<()>`

# Called by

- [parse_email_import](../../../../../../../functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import.md)