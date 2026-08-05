---
type: Rust Function
title: inbound_trace_advisory_lock_keys
resource: crates/lpe-storage/src/inbound.rs#L752-L761
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/inbound/lock_inbound_trace_delivery
---

# Signature

`fn inbound_trace_advisory_lock_keys(trace_id: &str) -> (i32, i32)`

# Called by

- [lock_inbound_trace_delivery](../../../../../functions/crates/lpe-storage/src/inbound/lock_inbound_trace_delivery.md)