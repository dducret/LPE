---
type: Rust Method
title: upsert_ews_sharing_grant
resource: crates/lpe-exchange/src/tests/mod.rs#L5720-L5787
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/accept_sharing_invitation
---

# Signature

`fn upsert_ews_sharing_grant<'a>( &'a self, owner_account_id: Uuid, grantee_email: &'a str, kind: CollaborationResourceKind, rights: CollaborationRights, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, CollaborationGrant>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [accept_sharing_invitation](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/accept_sharing_invitation.md)