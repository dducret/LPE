---
type: Rust Function
title: collaboration_sync_state_collection_id
resource: crates/lpe-exchange/src/service/ews/sync_state.rs#L670-L678
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_collection_id
---

# Signature

`pub(in crate::service) fn collaboration_sync_state_collection_id<'a>( sync_state: &'a str, kind: &str, ) -> Option<&'a str>`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [requested_sync_collection_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_collection_id.md)