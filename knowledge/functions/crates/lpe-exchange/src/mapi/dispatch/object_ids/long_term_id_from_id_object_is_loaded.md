---
type: Rust Function
title: long_term_id_from_id_object_is_loaded
resource: crates/lpe-exchange/src/mapi/dispatch/object_ids.rs#L82-L89
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/long_term_id_from_id_scope_is_loaded
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/rop_long_term_id_from_id_response_for_scope
---

# Signature

`fn long_term_id_from_id_object_is_loaded(object_id: Option<u64>, scope: &str) -> bool`

# Calls

- [long_term_id_from_id_scope_is_loaded](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/long_term_id_from_id_scope_is_loaded.md)

# Called by

- [rop_long_term_id_from_id_response_for_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/rop_long_term_id_from_id_response_for_scope.md)