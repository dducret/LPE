---
type: Rust Function
title: email_submission_state_fingerprint
resource: crates/lpe-jmap/src/service/helpers.rs#L676-L689
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint
  called_by:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state_entries
---

# Signature

`pub(super) fn email_submission_state_fingerprint(submission: &JmapEmailSubmission) -> String`

# Calls

- [opaque_state_fingerprint](../../../../../../functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint.md)

# Called by

- [email_submission_object_state_entries](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state_entries.md)