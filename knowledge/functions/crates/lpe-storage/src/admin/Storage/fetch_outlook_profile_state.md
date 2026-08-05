---
type: Rust Method
title: fetch_outlook_profile_state
resource: crates/lpe-storage/src/admin.rs#L199-L270
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/admin/helpers/count_from_row
  - functions/crates/lpe-storage/src/admin/helpers/unsupported_client_local_profile_state
---

# Signature

`pub async fn fetch_outlook_profile_state( &self, account_id: Uuid, ) -> Result<OutlookProfileState>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [count_from_row](../../../../../../functions/crates/lpe-storage/src/admin/helpers/count_from_row.md)
- [unsupported_client_local_profile_state](../../../../../../functions/crates/lpe-storage/src/admin/helpers/unsupported_client_local_profile_state.md)