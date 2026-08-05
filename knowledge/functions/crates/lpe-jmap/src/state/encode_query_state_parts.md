---
type: Rust Function
title: encode_query_state_parts
resource: crates/lpe-jmap/src/state.rs#L457-L477
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/state/encode_query_state
  - functions/crates/lpe-jmap/src/state/encode_query_state_reference
---

# Signature

`fn encode_query_state_parts( account_id: Uuid, kind: &str, filter: Option<Value>, sort: Option<Vec<Value>>, state_id: Option<Uuid>, cursor: Option<i64>, ids: Vec<String>, ) -> Result<String>`

# Called by

- [encode_query_state](../../../../../functions/crates/lpe-jmap/src/state/encode_query_state.md)
- [encode_query_state_reference](../../../../../functions/crates/lpe-jmap/src/state/encode_query_state_reference.md)