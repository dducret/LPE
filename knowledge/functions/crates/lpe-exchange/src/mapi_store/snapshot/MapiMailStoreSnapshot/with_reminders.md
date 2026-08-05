---
type: Rust Method
title: with_reminders
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L441-L444
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_reminders_as_underlying_calendar_and_task_links
---

# Signature

`pub(crate) fn with_reminders(mut self, reminders: Vec<ClientReminder>) -> Self`

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [snapshot_projects_reminders_as_underlying_calendar_and_task_links](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_reminders_as_underlying_calendar_and_task_links.md)