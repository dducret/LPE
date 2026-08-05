---
type: Rust Function
title: microsoft_oxcdata_reminders_restriction
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L473-L485
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/microsoft_oxcdata_reminder_core_restriction
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop
---

# Signature

`fn microsoft_oxcdata_reminders_restriction(restriction: &MapiRestriction) -> bool`

# Calls

- [microsoft_oxcdata_reminder_core_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/microsoft_oxcdata_reminder_core_restriction.md)

# Called by

- [bounded_search_criteria_from_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop.md)