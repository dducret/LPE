---
type: Rust Function
title: record_mail_submission
resource: crates/lpe-admin-api/src/observability.rs#L184-L191
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-admin-api/src/integration/accept_smtp_submission
  - functions/crates/lpe-admin-api/src/workspace/submit_message_with_store
---

# Signature

`pub fn record_mail_submission(source: &str)`

# Calls

- [entry](../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [accept_smtp_submission](../../../../../functions/crates/lpe-admin-api/src/integration/accept_smtp_submission.md)
- [submit_message_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/submit_message_with_store.md)