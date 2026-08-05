---
type: Rust Function
title: apply_email_submission_query
resource: crates/lpe-jmap/src/mail/values.rs#L56-L77
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/mail/values/email_submission_matches_filter
  - functions/crates/lpe-jmap/src/mail/values/compare_email_submission_sort_key
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query_changes
---

# Signature

`pub(crate) fn apply_email_submission_query( submissions: &mut Vec<JmapEmailSubmission>, filter: Option<&EmailSubmissionQueryFilter>, sort: Option<&[EmailSubmissionQuerySort]>, )`

# Calls

- [email_submission_matches_filter](../../../../../../functions/crates/lpe-jmap/src/mail/values/email_submission_matches_filter.md)
- [compare_email_submission_sort_key](../../../../../../functions/crates/lpe-jmap/src/mail/values/compare_email_submission_sort_key.md)

# Called by

- [handle_email_submission_query](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query.md)
- [handle_email_submission_query_changes](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query_changes.md)