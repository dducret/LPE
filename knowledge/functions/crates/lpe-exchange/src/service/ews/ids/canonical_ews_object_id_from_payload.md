---
type: Rust Function
title: canonical_ews_object_id_from_payload
resource: crates/lpe-exchange/src/service/ews/ids.rs#L144-L149
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_canonical_id
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_convert_source
---

# Signature

`fn canonical_ews_object_id_from_payload(payload: &str) -> Result<CanonicalEwsObjectId>`

# Calls

- [canonical_ews_object_id_from_canonical_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_canonical_id.md)

# Called by

- [canonical_ews_object_id_from_convert_source](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_convert_source.md)