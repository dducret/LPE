---
type: Rust Function
title: session_detects_abandon_after_inbox_fai_query_rows_release
resource: crates/lpe-exchange/src/mapi/session/tests.rs#L567-L585
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/tests/principal
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_contents_table
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_associated_query_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_table_release_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_findrow_returned_content
---

# Signature

`fn session_detects_abandon_after_inbox_fai_query_rows_release()`

# Calls

- [principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/principal.md)
- [create_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)
- [remove_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)
- [record_inbox_associated_contents_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_contents_table.md)
- [record_last_inbox_associated_query_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_associated_query_context.md)
- [record_last_table_release_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_table_release_context.md)
- [record_inbox_associated_findrow_returned_content](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_findrow_returned_content.md)