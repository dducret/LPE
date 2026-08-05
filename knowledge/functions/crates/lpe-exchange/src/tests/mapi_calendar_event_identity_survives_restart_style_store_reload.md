---
type: Rust Function
title: mapi_calendar_event_identity_survives_restart_style_store_reload
resource: crates/lpe-exchange/src/tests/mod.rs#L2255-L2316
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/events_for_folder
---

# Signature

`async fn mapi_calendar_event_identity_survives_restart_style_store_reload()`

# Calls

- [load_mapi_mail_store](../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [events_for_folder](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/events_for_folder.md)