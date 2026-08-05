---
type: Rust Function
title: session_does_not_treat_findrow_delivered_fai_as_abandoned
resource: crates/lpe-exchange/src/mapi/session/tests.rs#L588-L607
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/tests/principal
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_contents_table
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_findrow_returned_content
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_associated_query_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_table_release_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table
---

# Signature

`fn session_does_not_treat_findrow_delivered_fai_as_abandoned()`

# Calls

- [principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/principal.md)
- [create_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)
- [remove_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)
- [record_inbox_associated_contents_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_contents_table.md)
- [record_inbox_associated_findrow_returned_content](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_findrow_returned_content.md)
- [record_last_inbox_associated_query_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_associated_query_context.md)
- [record_last_table_release_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_table_release_context.md)
- [record_inbox_normal_contents_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table.md)