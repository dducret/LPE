---
type: Rust Method
title: cache_named_property
resource: crates/lpe-exchange/src/mapi/session/named_properties.rs#L56-L98
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/normalize_named_property
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/cache_named_property_mapping_and_return_property_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_names_from_property_ids_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_query_named_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/debug_named_property_context_reports_session_and_unresolved_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/contents_table_named_property_context_reports_selected_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/outlook_view_descriptor_named_property_context_reports_calendar_lids
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_contract_fingerprint_bounds_large_named_property_registry
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_stale_sharing_named_property_alias
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_well_known_contact_email_named_property_alias
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_contact_view_email_alias
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_visible_inbox_view_property
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_calendar_common_aliases
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/store_named_property_mapping_rejects_session_collision
  - functions/crates/lpe-exchange/src/mapi/session/tests/cached_named_property_updates_bidirectional_registry
  - functions/crates/lpe-exchange/src/mapi/session/tests/cached_well_known_named_property_keeps_registered_dynamic_id
  - functions/crates/lpe-exchange/src/mapi/session/tests/cached_well_known_named_property_keeps_registered_reserved_range_id
---

# Signature

`pub(in crate::mapi) fn cache_named_property( &mut self, property_id: u16, property: MapiNamedProperty, ) -> Option<u16>`

# Calls

- [normalize_named_property](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/normalize_named_property.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [remove](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [cache_named_property_mapping_and_return_property_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/cache_named_property_mapping_and_return_property_id.md)
- [append_get_names_from_property_ids_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_names_from_property_ids_response.md)
- [append_get_property_ids_from_names_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)
- [append_query_named_properties_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_query_named_properties_response.md)
- [debug_named_property_context_reports_session_and_unresolved_properties](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/debug_named_property_context_reports_session_and_unresolved_properties.md)
- [contents_table_named_property_context_reports_selected_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/contents_table_named_property_context_reports_selected_columns.md)
- [outlook_view_descriptor_named_property_context_reports_calendar_lids](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/outlook_view_descriptor_named_property_context_reports_calendar_lids.md)
- [calendar_contract_fingerprint_bounds_large_named_property_registry](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_contract_fingerprint_bounds_large_named_property_registry.md)
- [table_columns_normalize_stale_sharing_named_property_alias](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_stale_sharing_named_property_alias.md)
- [table_columns_normalize_well_known_contact_email_named_property_alias](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_well_known_contact_email_named_property_alias.md)
- [table_columns_normalize_outlook_contact_view_email_alias](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_contact_view_email_alias.md)
- [table_columns_normalize_outlook_visible_inbox_view_property](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_visible_inbox_view_property.md)
- [table_columns_normalize_outlook_calendar_common_aliases](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_calendar_common_aliases.md)
- [store_named_property_mapping_rejects_session_collision](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/store_named_property_mapping_rejects_session_collision.md)
- [cached_named_property_updates_bidirectional_registry](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/cached_named_property_updates_bidirectional_registry.md)
- [cached_well_known_named_property_keeps_registered_dynamic_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/cached_well_known_named_property_keeps_registered_dynamic_id.md)
- [cached_well_known_named_property_keeps_registered_reserved_range_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/cached_well_known_named_property_keeps_registered_reserved_range_id.md)