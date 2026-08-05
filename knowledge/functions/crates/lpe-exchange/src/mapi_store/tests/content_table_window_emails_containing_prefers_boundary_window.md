---
type: Rust Function
title: content_table_window_emails_containing_prefers_boundary_window
resource: crates/lpe-exchange/src/mapi_store/tests.rs#L445-L507
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_content_windows
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/content_table_window_emails_containing
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn content_table_window_emails_containing_prefers_boundary_window()`

# Calls

- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [with_content_windows](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_content_windows.md)
- [content_table_window_emails_containing](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/content_table_window_emails_containing.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)