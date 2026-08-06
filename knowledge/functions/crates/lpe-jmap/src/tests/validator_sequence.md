---
type: Rust Function
title: validator_sequence
resource: crates/lpe-jmap/src/tests.rs#L163-L170
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tests/validator_ok
  - functions/crates/lpe-jmap/src/tests/email_import_validates_and_preserves_multipart_attachments
  - functions/crates/lpe-jmap/src/tests/blob_upload_get_and_copy_resolve_created_blob_references
---

# Signature

`fn validator_sequence(results: Vec<Result<MagikaDetection, String>>) -> Validator<FakeDetector>`

# Called by

- [validator_ok](../../../../../functions/crates/lpe-jmap/src/tests/validator_ok.md)
- [email_import_validates_and_preserves_multipart_attachments](../../../../../functions/crates/lpe-jmap/src/tests/email_import_validates_and_preserves_multipart_attachments.md)
- [blob_upload_get_and_copy_resolve_created_blob_references](../../../../../functions/crates/lpe-jmap/src/tests/blob_upload_get_and_copy_resolve_created_blob_references.md)