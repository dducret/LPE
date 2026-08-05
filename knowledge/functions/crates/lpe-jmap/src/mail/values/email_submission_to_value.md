---
type: Rust Function
title: email_submission_to_value
resource: crates/lpe-jmap/src/mail/values.rs#L487-L539
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/insert_if
---

# Signature

`pub(crate) fn email_submission_to_value( submission: &JmapEmailSubmission, properties: &HashSet<String>, ) -> Value`

# Calls

- [insert_if](../../../../../../functions/crates/lpe-jmap/src/convert/insert_if.md)