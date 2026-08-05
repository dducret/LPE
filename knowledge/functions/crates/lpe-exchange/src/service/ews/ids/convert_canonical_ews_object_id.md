---
type: Rust Function
title: convert_canonical_ews_object_id
resource: crates/lpe-exchange/src/service/ews/ids.rs#L195-L215
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/ids/normalize_convert_id_format
  - functions/crates/lpe-exchange/src/service/ews/ids/opaque_ews_id
  - functions/crates/lpe-exchange/src/service/ews/ids/encode_hex_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ids/ExchangeService/convert_id
---

# Signature

`pub(in crate::service) fn convert_canonical_ews_object_id( canonical: &CanonicalEwsObjectId, destination_format: &str, ) -> Result<ConvertIdOutput>`

# Calls

- [normalize_convert_id_format](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/normalize_convert_id_format.md)
- [opaque_ews_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/opaque_ews_id.md)
- [encode_hex_entry_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/encode_hex_entry_id.md)

# Called by

- [convert_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/ExchangeService/convert_id.md)