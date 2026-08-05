---
type: Rust Method
title: accessible_shared_collection
resource: crates/lpe-exchange/src/service/ews/sharing.rs#L142-L164
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/get_sharing_folder
---

# Signature

`async fn accessible_shared_collection( &self, principal: &AccountPrincipal, owner_account_id: Uuid, kind: CollaborationResourceKind, ) -> Result<Option<CollaborationCollection>>`

# Called by

- [get_sharing_folder](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/get_sharing_folder.md)