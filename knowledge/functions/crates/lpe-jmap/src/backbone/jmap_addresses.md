---
type: Rust Function
title: jmap_addresses
resource: crates/lpe-jmap/src/backbone.rs#L156-L164
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/backbone/JmapEmailObject/from_canonical
---

# Signature

`fn jmap_addresses(addresses: &[JmapEmailAddress]) -> Vec<JmapAddressObject>`

# Called by

- [from_canonical](../../../../../functions/crates/lpe-jmap/src/backbone/JmapEmailObject/from_canonical.md)