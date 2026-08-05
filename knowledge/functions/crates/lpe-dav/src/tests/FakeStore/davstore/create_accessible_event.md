---
type: Rust Method
title: create_accessible_event
resource: crates/lpe-dav/src/tests.rs#L463-L519
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn create_accessible_event<'a>( &'a self, _principal_account_id: Uuid, collection_id: Option<&'a str>, input: UpsertClientEventInput, ) -> lpe_mail_auth::StoreFuture<'a, AccessibleEvent>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)