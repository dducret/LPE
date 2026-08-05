---
type: Rust Function
title: empty_appointment_tombstone
resource: crates/lpe-exchange/src/tests/mapi_over_http/free_busy.rs#L3-L11
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/free_busy/mapi_over_http_local_freebusy_accepts_outlook_tombstone_maintenance_sequence
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/free_busy/mapi_over_http_local_freebusy_rejects_nonempty_tombstone_without_canonical_mapping
---

# Signature

`fn empty_appointment_tombstone() -> Vec<u8>`

# Called by

- [mapi_over_http_local_freebusy_accepts_outlook_tombstone_maintenance_sequence](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/free_busy/mapi_over_http_local_freebusy_accepts_outlook_tombstone_maintenance_sequence.md)
- [mapi_over_http_local_freebusy_rejects_nonempty_tombstone_without_canonical_mapping](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/free_busy/mapi_over_http_local_freebusy_rejects_nonempty_tombstone_without_canonical_mapping.md)