---
type: Rust Method
title: create_accessible_event
resource: crates/lpe-exchange/src/tests/mod.rs#L8929-L8967
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/rights
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn create_accessible_event<'a>( &'a self, principal_account_id: Uuid, collection_id: Option<&'a str>, input: UpsertClientEventInput, ) -> StoreFuture<'a, AccessibleEvent>`

# Calls

- [rights](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/rights.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)