---
type: Rust Function
title: submit_request
resource: crates/lpe-admin-api/src/workspace/tests.rs#L629-L651
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/tests/delegated_send_on_behalf_defaults_sender_to_authenticated_account
  - functions/crates/lpe-admin-api/src/workspace/tests/delegated_send_as_without_explicit_sender_keeps_sender_empty
  - functions/crates/lpe-admin-api/src/workspace/tests/explicit_sender_fields_are_preserved
  - functions/crates/lpe-admin-api/src/workspace/tests/map_submit_message_request_preserves_web_submission_source
  - functions/crates/lpe-admin-api/src/workspace/tests/submit_message_handler_uses_canonical_submission_store_path
---

# Signature

`fn submit_request() -> SubmitMessageRequest`

# Called by

- [delegated_send_on_behalf_defaults_sender_to_authenticated_account](../../../../../../functions/crates/lpe-admin-api/src/workspace/tests/delegated_send_on_behalf_defaults_sender_to_authenticated_account.md)
- [delegated_send_as_without_explicit_sender_keeps_sender_empty](../../../../../../functions/crates/lpe-admin-api/src/workspace/tests/delegated_send_as_without_explicit_sender_keeps_sender_empty.md)
- [explicit_sender_fields_are_preserved](../../../../../../functions/crates/lpe-admin-api/src/workspace/tests/explicit_sender_fields_are_preserved.md)
- [map_submit_message_request_preserves_web_submission_source](../../../../../../functions/crates/lpe-admin-api/src/workspace/tests/map_submit_message_request_preserves_web_submission_source.md)
- [submit_message_handler_uses_canonical_submission_store_path](../../../../../../functions/crates/lpe-admin-api/src/workspace/tests/submit_message_handler_uses_canonical_submission_store_path.md)