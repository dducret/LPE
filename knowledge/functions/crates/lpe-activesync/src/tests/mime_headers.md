---
type: Rust Function
title: mime_headers
resource: crates/lpe-activesync/src/tests.rs#L1332-L1339
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/tests/base64_send_mail_request_dispatches
  - functions/crates/lpe-activesync/src/tests/send_mail_uses_canonical_submission_model
  - functions/crates/lpe-activesync/src/tests/send_mail_uses_on_behalf_sender_for_delegated_mailbox
  - functions/crates/lpe-activesync/src/tests/send_mail_rejects_inaccessible_shared_mailbox_address
  - functions/crates/lpe-activesync/src/tests/send_mail_decodes_multipart_and_encoded_headers
  - functions/crates/lpe-activesync/src/tests/benchmark_sync_refresh_and_submission_paths
---

# Signature

`fn mime_headers() -> HeaderMap`

# Called by

- [base64_send_mail_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_send_mail_request_dispatches.md)
- [send_mail_uses_canonical_submission_model](../../../../../functions/crates/lpe-activesync/src/tests/send_mail_uses_canonical_submission_model.md)
- [send_mail_uses_on_behalf_sender_for_delegated_mailbox](../../../../../functions/crates/lpe-activesync/src/tests/send_mail_uses_on_behalf_sender_for_delegated_mailbox.md)
- [send_mail_rejects_inaccessible_shared_mailbox_address](../../../../../functions/crates/lpe-activesync/src/tests/send_mail_rejects_inaccessible_shared_mailbox_address.md)
- [send_mail_decodes_multipart_and_encoded_headers](../../../../../functions/crates/lpe-activesync/src/tests/send_mail_decodes_multipart_and_encoded_headers.md)
- [benchmark_sync_refresh_and_submission_paths](../../../../../functions/crates/lpe-activesync/src/tests/benchmark_sync_refresh_and_submission_paths.md)