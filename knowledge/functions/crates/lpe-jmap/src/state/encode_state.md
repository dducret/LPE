---
type: Rust Function
title: encode_state
resource: crates/lpe-jmap/src/state.rs#L329-L335
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/encode_state_with_cursor
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_get
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_get
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/email_delivery_object_state
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/identity_object_state
  - functions/crates/lpe-jmap/src/state/changes_response_returns_intermediate_state_when_truncated
  - functions/crates/lpe-jmap/src/state/changes_response_rejects_invalid_or_mismatched_state_tokens
  - functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state
---

# Signature

`pub(crate) fn encode_state( account_id: Uuid, kind: &str, entries: Vec<StateEntry>, ) -> Result<String>`

# Calls

- [encode_state_with_cursor](../../../../../functions/crates/lpe-jmap/src/state/encode_state_with_cursor.md)

# Called by

- [handle_email_submission_get](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_get.md)
- [handle_identity_get](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_get.md)
- [canonical_object_state](../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state.md)
- [email_delivery_object_state](../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/email_delivery_object_state.md)
- [identity_object_state](../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/identity_object_state.md)
- [changes_response_returns_intermediate_state_when_truncated](../../../../../functions/crates/lpe-jmap/src/state/changes_response_returns_intermediate_state_when_truncated.md)
- [changes_response_rejects_invalid_or_mismatched_state_tokens](../../../../../functions/crates/lpe-jmap/src/state/changes_response_rejects_invalid_or_mismatched_state_tokens.md)
- [mail_push_type_state](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state.md)