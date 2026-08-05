---
type: Rust Function
title: apply_event_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/event_transactions.rs#L268-L335
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_pending_event_property_values
---

# Signature

`fn apply_event_property_values( pending: &mut HashMap<u32, MapiValue>, deleted: &mut HashSet<u32>, values: &[(u32, MapiValue)], )`

# Calls

- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [stage_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_values.md)
- [stage_pending_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_pending_event_property_values.md)