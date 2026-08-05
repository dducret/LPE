---
type: Rust Function
title: email_state_fingerprint
resource: crates/lpe-jmap/src/service/helpers.rs#L812-L842
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint
  called_by:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state_entries_with_bcc
---

# Signature

`pub(super) fn email_state_fingerprint(email: &JmapEmail, include_bcc: bool) -> String`

# Calls

- [opaque_state_fingerprint](../../../../../../functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint.md)

# Called by

- [mail_object_state_entries_with_bcc](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state_entries_with_bcc.md)