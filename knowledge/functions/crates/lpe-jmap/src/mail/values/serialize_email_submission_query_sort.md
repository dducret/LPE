---
type: Rust Function
title: serialize_email_submission_query_sort
resource: crates/lpe-jmap/src/mail/values.rs#L130-L140
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query_changes
---

# Signature

`pub(crate) fn serialize_email_submission_query_sort( sort: Option<&[EmailSubmissionQuerySort]>, ) -> Result<Option<Vec<Value>>>`

# Called by

- [handle_email_submission_query_changes](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query_changes.md)