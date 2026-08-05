---
type: Rust Function
title: outlook_profile_state_with_store
resource: crates/lpe-admin-api/src/workspace.rs#L1337-L1346
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/require_account_from_store
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/outlook_profile_state
  - functions/crates/lpe-admin-api/src/workspace/tests/outlook_profile_api_helper_reads_canonical_profile_state
---

# Signature

`async fn outlook_profile_state_with_store<S: ClientOutlookStore>( storage: &S, headers: &HeaderMap, ) -> std::result::Result<OutlookProfileState, (StatusCode, String)>`

# Calls

- [require_account_from_store](../../../../../functions/crates/lpe-admin-api/src/workspace/require_account_from_store.md)

# Called by

- [outlook_profile_state](../../../../../functions/crates/lpe-admin-api/src/workspace/outlook_profile_state.md)
- [outlook_profile_api_helper_reads_canonical_profile_state](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/outlook_profile_api_helper_reads_canonical_profile_state.md)