---
type: Rust Function
title: root_inclusive_idset
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L1262-L1266
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation
---

# Signature

`fn root_inclusive_idset(existing_counters: &[u64], root_counter: u64) -> Vec<u8>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [replguid_idset_from_counters](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters.md)

# Called by

- [hierarchy_semantic_validation](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation.md)