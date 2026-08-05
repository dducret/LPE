---
type: Rust Function
title: reminder_patch_has_changes
resource: crates/lpe-storage/src/mapi_events.rs#L997-L1001
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/update_mapi_event_reminder_in_tx
---

# Signature

`fn reminder_patch_has_changes(reminder: &MapiEventReminderPatch) -> bool`

# Called by

- [update_mapi_event_reminder_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_events/update_mapi_event_reminder_in_tx.md)