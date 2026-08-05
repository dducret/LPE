---
type: Rust Function
title: estimate_generated_message_size
resource: crates/lpe-storage/src/inbound.rs#L763-L773
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/dispatch_sieve_followups
---

# Signature

`fn estimate_generated_message_size( subject: &str, body_text: &str, attachments: &[AttachmentUploadInput], ) -> i64`

# Called by

- [dispatch_sieve_followups](../../../../../functions/crates/lpe-storage/src/inbound/Storage/dispatch_sieve_followups.md)