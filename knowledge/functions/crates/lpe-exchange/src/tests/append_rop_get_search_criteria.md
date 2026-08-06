---
type: Rust Function
title: append_rop_get_search_criteria
resource: crates/lpe-exchange/src/tests/mod.rs#L15172-L15174
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_received_date_bounds
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_attachment_exists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_read_bitmask
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_string8_body_content
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_get_search_criteria_rejects_exchange_only_blob_definition
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_builtin_contacts_search_get_search_criteria_uses_fixed_folder_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_set_get_search_criteria_updates_canonical_search_folder
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_folder_search_criteria_example_round_trips_message_class_and_importance
---

# Signature

`fn append_rop_get_search_criteria(rops: &mut Vec<u8>, input: u8)`

# Called by

- [mapi_over_http_set_get_search_criteria_round_trips_received_date_bounds](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_received_date_bounds.md)
- [mapi_over_http_set_get_search_criteria_round_trips_attachment_exists](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_attachment_exists.md)
- [mapi_over_http_set_get_search_criteria_round_trips_read_bitmask](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_read_bitmask.md)
- [mapi_over_http_set_get_search_criteria_round_trips_string8_body_content](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_string8_body_content.md)
- [mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses.md)
- [mapi_over_http_get_search_criteria_rejects_exchange_only_blob_definition](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_get_search_criteria_rejects_exchange_only_blob_definition.md)
- [mapi_over_http_builtin_contacts_search_get_search_criteria_uses_fixed_folder_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_builtin_contacts_search_get_search_criteria_uses_fixed_folder_id.md)
- [mapi_over_http_set_get_search_criteria_updates_canonical_search_folder](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_set_get_search_criteria_updates_canonical_search_folder.md)
- [mapi_over_http_microsoft_folder_search_criteria_example_round_trips_message_class_and_importance](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_folder_search_criteria_example_round_trips_message_class_and_importance.md)