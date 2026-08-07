---
type: Rust Function
title: outlook_profile_state
resource: crates/lpe-admin-api/src/workspace.rs#L1150-L1157
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/outlook_profile_state_with_store
---

# Signature

`pub(crate) async fn outlook_profile_state( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<OutlookProfileState>`

# Calls

- [outlook_profile_state_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/outlook_profile_state_with_store.md)