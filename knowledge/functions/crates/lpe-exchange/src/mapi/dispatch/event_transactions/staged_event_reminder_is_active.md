---
type: Rust Function
title: staged_event_reminder_is_active
resource: crates/lpe-exchange/src/mapi/dispatch/event_transactions.rs#L501-L510
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions
---

# Signature

`fn staged_event_reminder_is_active( transaction: &MapiEventTransaction, reminder: Option<&lpe_storage::ClientReminder>, ) -> bool`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [stage_event_property_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions.md)