---
type: Rust Function
title: canonical_ews_object_id_from_canonical_id
resource: crates/lpe-exchange/src/service/ews/ids.rs#L151-L180
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_family
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_convert_source
  - functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_payload
---

# Signature

`fn canonical_ews_object_id_from_canonical_id(id: &str) -> Result<CanonicalEwsObjectId>`

# Calls

- [canonical_ews_family](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_family.md)

# Called by

- [canonical_ews_object_id_from_convert_source](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_convert_source.md)
- [canonical_ews_object_id_from_payload](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_payload.md)