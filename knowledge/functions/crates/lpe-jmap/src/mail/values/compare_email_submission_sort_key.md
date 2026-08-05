---
type: Rust Function
title: compare_email_submission_sort_key
resource: crates/lpe-jmap/src/mail/values.rs#L116-L128
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/values/apply_email_submission_query
---

# Signature

`fn compare_email_submission_sort_key( left: &JmapEmailSubmission, right: &JmapEmailSubmission, property: &str, ) -> Ordering`

# Called by

- [apply_email_submission_query](../../../../../../functions/crates/lpe-jmap/src/mail/values/apply_email_submission_query.md)