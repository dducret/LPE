---
type: Rust Method
title: drop
resource: crates/lpe-storage/tests/mapi_contact_create.rs#L57-L105
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`fn drop(&mut self)`

# Calls

- [build](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)
- [context](../../../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)