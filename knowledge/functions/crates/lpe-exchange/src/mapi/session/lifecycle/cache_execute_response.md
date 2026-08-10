---
type: Rust Function
title: cache_execute_response
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L411-L444
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/session/tests/execute_replay_cache_evicts_oldest_inserted_request_id
---

# Signature

`pub(in crate::mapi) fn cache_execute_response( session: &mut MapiSession, request_id: &str, rop_fingerprint: u64, response_body: &[u8], request_rop_ids: String, response_rop_ids: String, response_rop_results: String, response_rop_buffer_bytes: usize, )`

# Calls

- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [execute_replay_cache_evicts_oldest_inserted_request_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/execute_replay_cache_evicts_oldest_inserted_request_id.md)