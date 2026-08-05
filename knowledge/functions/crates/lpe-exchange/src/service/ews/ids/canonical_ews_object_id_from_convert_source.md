---
type: Rust Function
title: canonical_ews_object_id_from_convert_source
resource: crates/lpe-exchange/src/service/ews/ids.rs#L121-L142
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/ids/decode_hex_entry_id
  - functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_payload
  - functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_canonical_id
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ids/ExchangeService/convert_id
---

# Signature

`pub(in crate::service) fn canonical_ews_object_id_from_convert_source( source: &ConvertIdSource, ) -> Result<CanonicalEwsObjectId>`

# Calls

- [decode_hex_entry_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/decode_hex_entry_id.md)
- [canonical_ews_object_id_from_payload](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_payload.md)
- [canonical_ews_object_id_from_canonical_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_canonical_id.md)

# Called by

- [convert_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/ExchangeService/convert_id.md)