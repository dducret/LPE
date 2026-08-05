---
type: Rust Function
title: classifier_requires_inbox_query_rows_after_inbox_table_open
resource: crates/lpe-exchange/src/mapi/outlook_startup.rs#L213-L238
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/tests/test_session_for_outlook_startup
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_opened_folder
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_contents_table
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_exact_findrow
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_query_rows_returned_non_empty
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_receive_folder_verification_passed
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_normal_contents_table_query_rows
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary
---

# Signature

`fn classifier_requires_inbox_query_rows_after_inbox_table_open()`

# Calls

- [test_session_for_outlook_startup](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/test_session_for_outlook_startup.md)
- [record_opened_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_opened_folder.md)
- [record_inbox_associated_contents_table](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_contents_table.md)
- [record_inbox_associated_exact_findrow](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_exact_findrow.md)
- [record_inbox_associated_query_rows_returned_non_empty](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_query_rows_returned_non_empty.md)
- [record_receive_folder_verification_passed](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_receive_folder_verification_passed.md)
- [record_inbox_normal_contents_table](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table.md)
- [record_default_view_normal_contents_table_query_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_normal_contents_table_query_rows.md)
- [outlook_startup_gate_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary.md)