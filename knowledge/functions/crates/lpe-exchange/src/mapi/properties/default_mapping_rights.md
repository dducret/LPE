---
type: Rust Function
title: default_mapping_rights
resource: crates/lpe-exchange/src/mapi/properties.rs#L1320-L1327
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_query_position_summary_projects_observed_outlook_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_query_position_summary_flags_zero_duration_timed_events
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/default_event_for_mapping
  - functions/crates/lpe-exchange/src/mapi/properties/contact/default_contact_for_mapping
  - functions/crates/lpe-exchange/src/mapi/properties/task/default_task_for_mapping
  - functions/crates/lpe-exchange/src/mapi/properties/tests/contact_and_task_access_follow_effective_canonical_rights
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_contact_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_event_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_task_row
---

# Signature

`pub(in crate::mapi) fn default_mapping_rights() -> CollaborationRights`

# Called by

- [calendar_query_position_summary_projects_observed_outlook_columns](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_query_position_summary_projects_observed_outlook_columns.md)
- [calendar_query_position_summary_flags_zero_duration_timed_events](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_query_position_summary_flags_zero_duration_timed_events.md)
- [default_event_for_mapping](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/default_event_for_mapping.md)
- [default_contact_for_mapping](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/default_contact_for_mapping.md)
- [default_task_for_mapping](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/default_task_for_mapping.md)
- [contact_and_task_access_follow_effective_canonical_rights](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/contact_and_task_access_follow_effective_canonical_rights.md)
- [serialize_pending_contact_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_contact_row.md)
- [serialize_pending_event_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_event_row.md)
- [serialize_pending_task_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_task_row.md)