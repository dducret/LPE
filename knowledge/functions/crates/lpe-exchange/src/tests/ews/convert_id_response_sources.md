---
type: Rust Function
title: convert_id_response_sources
resource: crates/lpe-exchange/src/tests/ews.rs#L4974-L4987
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/ews/convert_id_response_sources_for_element
  called_by:
  - functions/crates/lpe-exchange/src/tests/ews/convert_id_round_trips_supported_canonical_object_families
  - functions/crates/lpe-exchange/src/tests/ews/convert_id_round_trips_hex_entry_id_attachment_payload
---

# Signature

`fn convert_id_response_sources(body: &str) -> Vec<(String, String, String)>`

# Calls

- [convert_id_response_sources_for_element](../../../../../../functions/crates/lpe-exchange/src/tests/ews/convert_id_response_sources_for_element.md)

# Called by

- [convert_id_round_trips_supported_canonical_object_families](../../../../../../functions/crates/lpe-exchange/src/tests/ews/convert_id_round_trips_supported_canonical_object_families.md)
- [convert_id_round_trips_hex_entry_id_attachment_payload](../../../../../../functions/crates/lpe-exchange/src/tests/ews/convert_id_round_trips_hex_entry_id_attachment_payload.md)