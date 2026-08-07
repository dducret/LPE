---
type: Rust Function
title: opaque_state_fingerprint
resource: crates/lpe-jmap/src/service/helpers.rs#L895-L901
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_changes
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state
  - functions/crates/lpe-jmap/src/service/helpers/collection_state_fingerprint
  - functions/crates/lpe-jmap/src/service/helpers/email_submission_state_fingerprint
  - functions/crates/lpe-jmap/src/service/helpers/identity_state_fingerprint
  - functions/crates/lpe-jmap/src/service/helpers/mailbox_state_fingerprint
  - functions/crates/lpe-jmap/src/service/helpers/contact_state_fingerprint
  - functions/crates/lpe-jmap/src/service/helpers/event_state_fingerprint
  - functions/crates/lpe-jmap/src/service/helpers/task_state_fingerprint
  - functions/crates/lpe-jmap/src/service/helpers/task_list_state_fingerprint
  - functions/crates/lpe-jmap/src/service/helpers/email_state_fingerprint
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/email_delivery_object_state
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state_entries_with_bcc
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries
  - functions/crates/lpe-jmap/src/session/session_state
  - functions/crates/lpe-jmap/src/vacation/vacation_response_state
---

# Signature

`pub(crate) fn opaque_state_fingerprint(value: &str) -> String`

# Called by

- [handle_canonical_changes](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_changes.md)
- [canonical_object_state](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state.md)
- [collection_state_fingerprint](../../../../../../functions/crates/lpe-jmap/src/service/helpers/collection_state_fingerprint.md)
- [email_submission_state_fingerprint](../../../../../../functions/crates/lpe-jmap/src/service/helpers/email_submission_state_fingerprint.md)
- [identity_state_fingerprint](../../../../../../functions/crates/lpe-jmap/src/service/helpers/identity_state_fingerprint.md)
- [mailbox_state_fingerprint](../../../../../../functions/crates/lpe-jmap/src/service/helpers/mailbox_state_fingerprint.md)
- [contact_state_fingerprint](../../../../../../functions/crates/lpe-jmap/src/service/helpers/contact_state_fingerprint.md)
- [event_state_fingerprint](../../../../../../functions/crates/lpe-jmap/src/service/helpers/event_state_fingerprint.md)
- [task_state_fingerprint](../../../../../../functions/crates/lpe-jmap/src/service/helpers/task_state_fingerprint.md)
- [task_list_state_fingerprint](../../../../../../functions/crates/lpe-jmap/src/service/helpers/task_list_state_fingerprint.md)
- [email_state_fingerprint](../../../../../../functions/crates/lpe-jmap/src/service/helpers/email_state_fingerprint.md)
- [email_delivery_object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/email_delivery_object_state.md)
- [mail_object_state_entries_with_bcc](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state_entries_with_bcc.md)
- [object_state_entries](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries.md)
- [session_state](../../../../../../functions/crates/lpe-jmap/src/session/session_state.md)
- [vacation_response_state](../../../../../../functions/crates/lpe-jmap/src/vacation/vacation_response_state.md)