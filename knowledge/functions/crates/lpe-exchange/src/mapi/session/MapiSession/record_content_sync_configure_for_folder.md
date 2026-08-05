---
type: Rust Method
title: record_content_sync_configure_for_folder
resource: crates/lpe-exchange/src/mapi/session.rs#L1085-L1091
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_content_sync_configure
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress
---

# Signature

`pub(in crate::mapi) fn record_content_sync_configure_for_folder(&mut self, folder_id: u64)`

# Calls

- [record_content_sync_configure](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_content_sync_configure.md)

# Called by

- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress.md)
- [classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress.md)