---
type: Rust Method
title: fetch_jmap_email_submissions
resource: crates/lpe-storage/src/protocols.rs#L1226-L1329
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/submission/types/sender_identity_id
  - functions/crates/lpe-storage/src/submission/types/sender_authorization_kind_from_str
---

# Signature

`pub async fn fetch_jmap_email_submissions( &self, account_id: Uuid, ids: &[Uuid], ) -> Result<Vec<JmapEmailSubmission>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [sender_identity_id](../../../../../../functions/crates/lpe-storage/src/submission/types/sender_identity_id.md)
- [sender_authorization_kind_from_str](../../../../../../functions/crates/lpe-storage/src/submission/types/sender_authorization_kind_from_str.md)