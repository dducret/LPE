---
type: Rust Function
title: persisted_message_delete_is_best_effort
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L58-L60
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/persisted_object_property_delete_is_idempotent
---

# Signature

`pub(super) fn persisted_message_delete_is_best_effort(object: Option<&MapiObject>) -> bool`

# Called by

- [persisted_object_property_delete_is_idempotent](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/persisted_object_property_delete_is_idempotent.md)