---
type: Rust Function
title: inbox_contents_invariant_accepts_message_identity_columns
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L332-L398
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/test_table_email
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
---

# Signature

`fn inbox_contents_invariant_accepts_message_identity_columns()`

# Calls

- [test_table_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/test_table_email.md)
- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)