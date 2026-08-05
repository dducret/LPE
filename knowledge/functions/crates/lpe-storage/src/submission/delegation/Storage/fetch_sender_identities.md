---
type: Rust Method
title: fetch_sender_identities
resource: crates/lpe-storage/src/submission/delegation.rs#L703-L808
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/submission/Storage/account_identity_for_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/submission/delegation/Storage/require_mailbox_account_access
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/submission/types/sender_identity_id
---

# Signature

`pub async fn fetch_sender_identities( &self, principal_account_id: Uuid, target_account_id: Uuid, ) -> Result<Vec<SenderIdentity>>`

# Calls

- [tenant_id_for_account_id](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [account_identity_for_id](../../../../../../../functions/crates/lpe-storage/src/submission/Storage/account_identity_for_id.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [require_mailbox_account_access](../../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/require_mailbox_account_access.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [sender_identity_id](../../../../../../../functions/crates/lpe-storage/src/submission/types/sender_identity_id.md)