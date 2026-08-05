---
type: Rust Method
title: fetch_outgoing_sender_delegation_grants
resource: crates/lpe-storage/src/submission/delegation.rs#L587-L620
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_outgoing_sender_delegation_grants( &self, owner_account_id: Uuid, ) -> Result<Vec<SenderDelegationGrant>>`

# Calls

- [tenant_id_for_account_id](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)