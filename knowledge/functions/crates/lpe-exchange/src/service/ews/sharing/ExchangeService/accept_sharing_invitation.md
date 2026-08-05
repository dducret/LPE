---
type: Rust Method
title: accept_sharing_invitation
resource: crates/lpe-exchange/src/service/ews/sharing.rs#L104-L140
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/sharing/parse_sharing_request
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/resolve_same_tenant_account
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_ews_sharing_grant
  - functions/crates/lpe-exchange/src/service/ews/sharing/accept_sharing_invitation_response
  - functions/crates/lpe-exchange/src/service/ews/ids/versioned_change_key
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item
---

# Signature

`pub(in crate::service) async fn accept_sharing_invitation( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [parse_sharing_request](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/parse_sharing_request.md)
- [resolve_same_tenant_account](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/resolve_same_tenant_account.md)
- [upsert_ews_sharing_grant](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_ews_sharing_grant.md)
- [accept_sharing_invitation_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/accept_sharing_invitation_response.md)
- [versioned_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/versioned_change_key.md)

# Called by

- [create_item](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)