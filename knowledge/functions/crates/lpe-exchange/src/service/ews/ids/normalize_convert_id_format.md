---
type: Rust Function
title: normalize_convert_id_format
resource: crates/lpe-exchange/src/service/ews/ids.rs#L217-L227
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ids/convert_canonical_ews_object_id
---

# Signature

`fn normalize_convert_id_format(format: &str) -> Result<&'static str>`

# Called by

- [convert_canonical_ews_object_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/convert_canonical_ews_object_id.md)