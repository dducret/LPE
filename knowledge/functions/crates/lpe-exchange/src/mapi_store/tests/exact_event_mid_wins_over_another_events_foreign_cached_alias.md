---
type: Rust Function
title: exact_event_mid_wins_over_another_events_foreign_cached_alias
resource: crates/lpe-exchange/src/mapi_store/tests.rs#L188-L214
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_store/tests/test_mapi_event
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/identity/forget_mapi_identity
---

# Signature

`fn exact_event_mid_wins_over_another_events_foreign_cached_alias()`

# Calls

- [empty](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [test_mapi_event](../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/test_mapi_event.md)
- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [event_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [forget_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/forget_mapi_identity.md)