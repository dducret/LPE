---
type: Rust Method
title: create_accessible_contact
resource: crates/lpe-dav/src/tests.rs#L420-L461
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn create_accessible_contact<'a>( &'a self, _principal_account_id: Uuid, collection_id: Option<&'a str>, input: UpsertClientContactInput, ) -> lpe_mail_auth::StoreFuture<'a, AccessibleContact>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)