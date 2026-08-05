---
type: Rust Function
title: recoverable_created_by_protocol
resource: crates/lpe-storage/src/mail_items.rs#L732-L740
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mail_items/delete_jmap_email_memberships
---

# Signature

`fn recoverable_created_by_protocol(audit_action: &str) -> &'static str`

# Called by

- [delete_jmap_email_memberships](../../../../../functions/crates/lpe-storage/src/mail_items/delete_jmap_email_memberships.md)