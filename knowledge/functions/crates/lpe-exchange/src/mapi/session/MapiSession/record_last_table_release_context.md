---
type: Rust Method
title: record_last_table_release_context
resource: crates/lpe-exchange/src/mapi/session.rs#L580-L583
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_detects_abandon_after_inbox_fai_query_rows_release
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_does_not_treat_findrow_delivered_fai_as_abandoned
---

# Signature

`pub(in crate::mapi) fn record_last_table_release_context(&mut self, context: String)`

# Called by

- [append_release_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)
- [session_detects_abandon_after_inbox_fai_query_rows_release](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_detects_abandon_after_inbox_fai_query_rows_release.md)
- [session_does_not_treat_findrow_delivered_fai_as_abandoned](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_does_not_treat_findrow_delivered_fai_as_abandoned.md)