---
type: Rust Function
title: classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress
resource: crates/lpe-exchange/src/mapi/outlook_startup.rs#L283-L312
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/tests/test_session_for_outlook_startup
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_opened_folder
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_contents_table
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_findrow_returned_content
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_receive_folder_verification_passed
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_content_sync_configure_for_folder
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary
---

# Signature

`fn classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress()`

# Calls

- [test_session_for_outlook_startup](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/test_session_for_outlook_startup.md)
- [record_opened_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_opened_folder.md)
- [record_inbox_associated_contents_table](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_contents_table.md)
- [record_inbox_associated_findrow_returned_content](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_findrow_returned_content.md)
- [record_receive_folder_verification_passed](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_receive_folder_verification_passed.md)
- [record_content_sync_configure_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_content_sync_configure_for_folder.md)
- [outlook_startup_gate_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary.md)