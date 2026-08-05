---
type: Rust Function
title: parse_header_recipients
resource: crates/lpe-storage/src/mail.rs#L64-L92
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mail/unfolded_headers
  called_by:
  - functions/crates/lpe-admin-api/src/integration/parse_required_submission_from
  - functions/crates/lpe-admin-api/src/integration/parse_smtp_submission_sender
  - functions/crates/lpe-admin-api/src/integration/merge_smtp_bcc_recipients
  - functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message
  - functions/crates/lpe-storage/src/mail/parse_header_recipients_unfolds_and_normalizes_addresses
---

# Signature

`pub fn parse_header_recipients( raw_message: &[u8], header_name: &str, ) -> Vec<SubmittedRecipientInput>`

# Calls

- [unfolded_headers](../../../../../functions/crates/lpe-storage/src/mail/unfolded_headers.md)

# Called by

- [parse_required_submission_from](../../../../../functions/crates/lpe-admin-api/src/integration/parse_required_submission_from.md)
- [parse_smtp_submission_sender](../../../../../functions/crates/lpe-admin-api/src/integration/parse_smtp_submission_sender.md)
- [merge_smtp_bcc_recipients](../../../../../functions/crates/lpe-admin-api/src/integration/merge_smtp_bcc_recipients.md)
- [deliver_inbound_message](../../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)
- [parse_header_recipients_unfolds_and_normalizes_addresses](../../../../../functions/crates/lpe-storage/src/mail/parse_header_recipients_unfolds_and_normalizes_addresses.md)