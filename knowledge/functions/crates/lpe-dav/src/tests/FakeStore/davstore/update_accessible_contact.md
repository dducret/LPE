---
type: Rust Method
title: update_accessible_contact
resource: crates/lpe-dav/src/tests.rs#L521-L560
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn update_accessible_contact<'a>( &'a self, _principal_account_id: Uuid, contact_id: Uuid, input: UpsertClientContactInput, ) -> lpe_mail_auth::StoreFuture<'a, AccessibleContact>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)