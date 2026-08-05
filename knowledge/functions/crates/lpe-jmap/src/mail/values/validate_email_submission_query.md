---
type: Rust Function
title: validate_email_submission_query
resource: crates/lpe-jmap/src/mail/values.rs#L31-L54
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/parse/parse_local_datetime
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query_changes
---

# Signature

`pub(crate) fn validate_email_submission_query( filter: Option<&EmailSubmissionQueryFilter>, sort: Option<&[EmailSubmissionQuerySort]>, ) -> Result<()>`

# Calls

- [parse_local_datetime](../../../../../../functions/crates/lpe-jmap/src/parse/parse_local_datetime.md)

# Called by

- [handle_email_submission_query](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query.md)
- [handle_email_submission_query_changes](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query_changes.md)