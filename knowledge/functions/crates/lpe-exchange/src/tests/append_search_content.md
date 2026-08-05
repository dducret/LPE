---
type: Rust Function
title: append_search_content
resource: crates/lpe-exchange/src/tests/mod.rs#L14885-L14891
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_set_get_search_criteria_updates_canonical_search_folder
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_microsoft_set_search_criteria_rejects_initial_empty_folder_scope
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_microsoft_set_search_criteria_rejects_scope_containing_search_folder
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_microsoft_set_search_criteria_reuses_previous_scope_and_restriction
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_set_search_criteria_rejects_unsupported_restriction
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_microsoft_set_search_criteria_rejects_invalid_search_flags
---

# Signature

`fn append_search_content(restriction: &mut Vec<u8>, property_tag: u32, value: &str)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [append_mapi_utf16_property](../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)

# Called by

- [mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses.md)
- [mapi_over_http_set_get_search_criteria_updates_canonical_search_folder](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_set_get_search_criteria_updates_canonical_search_folder.md)
- [mapi_over_http_microsoft_set_search_criteria_rejects_initial_empty_folder_scope](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_microsoft_set_search_criteria_rejects_initial_empty_folder_scope.md)
- [mapi_over_http_microsoft_set_search_criteria_rejects_scope_containing_search_folder](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_microsoft_set_search_criteria_rejects_scope_containing_search_folder.md)
- [mapi_over_http_microsoft_set_search_criteria_reuses_previous_scope_and_restriction](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_microsoft_set_search_criteria_reuses_previous_scope_and_restriction.md)
- [mapi_over_http_set_search_criteria_rejects_unsupported_restriction](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_set_search_criteria_rejects_unsupported_restriction.md)
- [mapi_over_http_microsoft_set_search_criteria_rejects_invalid_search_flags](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_microsoft_set_search_criteria_rejects_invalid_search_flags.md)