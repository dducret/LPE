---
type: Rust Method
title: update_accessible_event
resource: crates/lpe-dav/src/tests.rs#L562-L616
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn update_accessible_event<'a>( &'a self, _principal_account_id: Uuid, event_id: Uuid, input: UpsertClientEventInput, ) -> lpe_mail_auth::StoreFuture<'a, AccessibleEvent>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)