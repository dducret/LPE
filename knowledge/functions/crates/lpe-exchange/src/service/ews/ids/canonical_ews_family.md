---
type: Rust Function
title: canonical_ews_family
resource: crates/lpe-exchange/src/service/ews/ids.rs#L182-L193
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_canonical_id
---

# Signature

`fn canonical_ews_family(family: &str) -> Result<&'static str>`

# Called by

- [canonical_ews_object_id_from_canonical_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_canonical_id.md)