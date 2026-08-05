---
type: Rust Function
title: email_submission_matches_filter
resource: crates/lpe-jmap/src/mail/values.rs#L79-L114
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/values/apply_email_submission_query
---

# Signature

`fn email_submission_matches_filter( submission: &JmapEmailSubmission, filter: &EmailSubmissionQueryFilter, ) -> bool`

# Called by

- [apply_email_submission_query](../../../../../../functions/crates/lpe-jmap/src/mail/values/apply_email_submission_query.md)