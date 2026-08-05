---
type: Rust Function
title: log_nspi_get_props_debug
resource: crates/lpe-exchange/src/mapi/nspi/diagnostics.rs#L40-L97
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_stat_current_rec
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/format_nspi_property_tags_for_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
---

# Signature

`pub(super) fn log_nspi_get_props_debug( principal: &AccountPrincipal, request: &[u8], request_type: &str, raw_tag_candidates: &[u32], tags: &[u32], dropped_tags: &[u32], entry: Option<&ExchangeAddressBookEntry>, )`

# Calls

- [nspi_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id.md)
- [nspi_requested_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids.md)
- [nspi_stat_current_rec](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_stat_current_rec.md)
- [format_nspi_property_tags_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/format_nspi_property_tags_for_debug.md)

# Called by

- [nspi_props_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)