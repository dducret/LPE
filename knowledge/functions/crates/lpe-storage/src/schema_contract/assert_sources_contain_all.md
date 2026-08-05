---
type: Rust Function
title: assert_sources_contain_all
resource: crates/lpe-storage/src/schema_contract.rs#L133-L140
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/schema_contract/calendar_event_mutations_advance_canonical_and_mapi_versions
  - functions/crates/lpe-storage/src/schema_contract/mapi_property_store_runtime_sql_matches_durable_schema
  - functions/crates/lpe-storage/src/schema_contract/mapi_profile_settings_are_canonical_account_settings
  - functions/crates/lpe-storage/src/schema_contract/mapi_folder_profile_properties_are_bounded_profile_state
  - functions/crates/lpe-storage/src/schema_contract/mapi_special_folder_aliases_are_bounded_protocol_identity_metadata
---

# Signature

`fn assert_sources_contain_all(name: &str, sources: &[&str], needles: &[&str])`

# Called by

- [calendar_event_mutations_advance_canonical_and_mapi_versions](../../../../../functions/crates/lpe-storage/src/schema_contract/calendar_event_mutations_advance_canonical_and_mapi_versions.md)
- [mapi_property_store_runtime_sql_matches_durable_schema](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_property_store_runtime_sql_matches_durable_schema.md)
- [mapi_profile_settings_are_canonical_account_settings](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_profile_settings_are_canonical_account_settings.md)
- [mapi_folder_profile_properties_are_bounded_profile_state](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_folder_profile_properties_are_bounded_profile_state.md)
- [mapi_special_folder_aliases_are_bounded_protocol_identity_metadata](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_special_folder_aliases_are_bounded_protocol_identity_metadata.md)