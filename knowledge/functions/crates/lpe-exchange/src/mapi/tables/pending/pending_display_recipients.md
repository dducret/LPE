---
type: Rust Function
title: pending_display_recipients
resource: crates/lpe-exchange/src/mapi/tables/pending.rs#L331-L347
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_message_row
---

# Signature

`fn pending_display_recipients(recipients: &[PendingRecipient], recipient_type: u8) -> String`

# Called by

- [serialize_pending_message_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_message_row.md)