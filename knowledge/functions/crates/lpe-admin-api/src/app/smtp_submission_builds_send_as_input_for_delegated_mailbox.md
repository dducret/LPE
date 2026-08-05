---
type: Rust Function
title: smtp_submission_builds_send_as_input_for_delegated_mailbox
resource: crates/lpe-admin-api/src/app.rs#L877-L937
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mail/parse_rfc822_message
  - functions/crates/lpe-admin-api/src/integration/parse_smtp_submission_sender
  - functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input_for_owner
---

# Signature

`fn smtp_submission_builds_send_as_input_for_delegated_mailbox()`

# Calls

- [parse_rfc822_message](../../../../../functions/crates/lpe-storage/src/mail/parse_rfc822_message.md)
- [parse_smtp_submission_sender](../../../../../functions/crates/lpe-admin-api/src/integration/parse_smtp_submission_sender.md)
- [build_smtp_submission_input_for_owner](../../../../../functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input_for_owner.md)