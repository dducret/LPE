---
type: Rust Module
title: nspi
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1-L1569
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-properties-write-ascii-z-nspi-permanent-entry-id-provider-uid
  - external/super-rop
  - external/super-session
  - external/super-transport
  - external/super-wire-mapihttprequesttype-as-mapirequesttype
  - external/super
  - external/crate-store-exchangeaddressbookentrydetails
  - external/lpe-domain-normalization
  - external/std-collections-hashmap
  - external/std-sync-mutex-oncelock
  - external/diagnostics-format-nspi-duplicate-entry-keys-for-debug-format-nspi-entry-summaries-for-debug
  - external/diagnostics-log-nspi-dn-to-mid-debug-log-nspi-get-props-debug-log-nspi-response-contract-log-nspi-rowset-debug-nspi-raw-property-tag-candidates
  - external/dn-to-mid-parse-dn-to-mid-names
  - external/property-values-allocate-nspi-entry-identities-allocate-principal-nspi-identity-nspi-get-props-missing-property-value-list-nspi-get-props-property-tags-nspi-get-props-property-value-list-nspi-property-tags-response-nspi-resolved-entry-row-parse-nspi-get-props-request-nspi-bootstrap-property-tags
  - external/pub-in-crate-mapi-use-property-values-nspi-entry-available-property-tags-nspi-entry-display-type-nspi-entry-id-nspi-entry-property-value-list-nspi-known-unsupported-property-tag-name-nspi-property-tag-is-supported-nspi-requested-property-tags-principal-address-book-entry-principal-minimal-entry-id-write-address-book-tagged-property-value-write-large-property-tag-array-nspivalue
  - external/property-values-nspi-entry-value-nspi-additional-requested-property-tags-nspi-supported-request-types
  - external/special-tables-nspi-unicode-strings-flag
  - external/special-tables-nspi-hierarchy-info-response-nspi-special-table-response
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [handle_nspi_request](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request.md)
- [bind_response](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/bind_response.md)
- [endpoint_url_response](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/endpoint_url_response.md)
- [nspi_disabled_mutation_response](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_disabled_mutation_response.md)
- [resolve_names_response](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response.md)
- [resolve_names_columns](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_columns.md)
- [parse_resolve_names_columns](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_columns.md)
- [resolve_names_requested_values](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_requested_values.md)
- [parse_resolve_names_values](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_values.md)
- [nspi_u32_result_response](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_u32_result_response.md)
- [nspi_dn_to_mid_response](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response.md)
- [NspiGetPropListRequest](../../../../../classes/crates/lpe-exchange/src/mapi/nspi/NspiGetPropListRequest.md)
- [parse_nspi_get_prop_list_request](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/parse_nspi_get_prop_list_request.md)
- [nspi_get_prop_list_response](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response.md)
- [NspiDnToMidMatch](../../../../../classes/crates/lpe-exchange/src/mapi/nspi/NspiDnToMidMatch.md)
- [nspi_match_dn_to_mid_entry](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_dn_to_mid_entry.md)
- [nspi_dn_to_mid_match](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_match.md)
- [nspi_props_response](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)
- [nspi_rowset_response](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response.md)
- [nspi_query_rows_count](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_count.md)
- [nspi_query_rows_explicit_entry_ids](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_explicit_entry_ids.md)
- [NspiQueryRowsCountDetails](../../../../../classes/crates/lpe-exchange/src/mapi/nspi/NspiQueryRowsCountDetails.md)
- [nspi_query_rows_count_details](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_count_details.md)
- [nspi_request_type_is_query_rows](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_request_type_is_query_rows.md)
- [nspi_body_looks_like_query_rows](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_body_looks_like_query_rows.md)
- [nspi_query_rows_layout_from_body](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_layout_from_body.md)
- [nspi_query_rows_layout_at_offset](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_layout_at_offset.md)
- [nspi_matches_response](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response.md)
- [nspi_minimal_ids_response](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_minimal_ids_response.md)
- [nspi_template_info_response](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_template_info_response.md)
- [nspi_update_stat_response](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_update_stat_response.md)
- [nspi_entry_instance_key](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_instance_key.md)
- [nspi_entry_record_key](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_record_key.md)
- [nspi_entry_permanent_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_permanent_entry_id.md)
- [nspi_entry_search_key](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_search_key.md)
- [nspi_entry_legacy_dn](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_legacy_dn.md)
- [nspi_entry_unprefixed_legacy_dn](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn.md)
- [nspi_entry_legacy_dn_with_prefix](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_legacy_dn_with_prefix.md)
- [nspi_legacy_cn_from_source](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_legacy_cn_from_source.md)
- [nspi_legacy_dn_from_cn](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_legacy_dn_from_cn.md)
- [nspi_entry_alias](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_alias.md)
- [nspi_entry_is_principal](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_is_principal.md)
- [nspi_lookup_matches_principal](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_lookup_matches_principal.md)
- [principal_legacy_dn_aliases](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/principal_legacy_dn_aliases.md)
- [push_principal_legacy_dn_alias](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/push_principal_legacy_dn_alias.md)
- [nspi_requested_entry](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry.md)
- [nspi_request_has_entry_selector](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_request_has_entry_selector.md)
- [nspi_filter_entries_for_request](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_filter_entries_for_request.md)
- [nspi_filter_explicit_table_entries](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_filter_explicit_table_entries.md)
- [nspi_match_entry](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_entry.md)
- [nspi_ranked_matching_entries](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_ranked_matching_entries.md)
- [nspi_entry_kind_rank](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_kind_rank.md)
- [nspi_entry_match_rank](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_match_rank.md)
- [nspi_requested_entry_ids](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids.md)
- [push_unique_nspi_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/push_unique_nspi_entry_id.md)
- [nspi_stat_current_rec](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_stat_current_rec.md)
- [nspi_direct_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_direct_entry_id.md)
- [nspi_word_looks_like_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_word_looks_like_entry_id.md)
- [nspi_word_looks_like_property_tag](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_word_looks_like_property_tag.md)
- [scan_address_book_lookup_values](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/scan_address_book_lookup_values.md)
- [scan_ascii_lookup_values](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/scan_ascii_lookup_values.md)
- [scan_utf16_lookup_values](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/scan_utf16_lookup_values.md)
- [is_utf16_lookup_start](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/is_utf16_lookup_start.md)
- [decode_utf16le_string](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/decode_utf16le_string.md)
- [normalize_nspi_lookup_value](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value.md)
- [nspi_lookup_value_is_plausible](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_lookup_value_is_plausible.md)
- [public_endpoint_url](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/public_endpoint_url.md)

# Imports

- `super::properties::{write_ascii_z, NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID}`
- `super::rop::*`
- `super::session::*`
- `super::transport::*`
- `super::wire::MapiHttpRequestType as MapiRequestType`
- `super::*`
- `crate::store::ExchangeAddressBookEntryDetails`
- `lpe_domain::normalization`
- `std::collections::HashMap`
- `std::sync::{Mutex, OnceLock}`
- `diagnostics::{
    format_nspi_duplicate_entry_keys_for_debug, format_nspi_entry_summaries_for_debug,
}`
- `diagnostics::{
    log_nspi_dn_to_mid_debug, log_nspi_get_props_debug, log_nspi_response_contract,
    log_nspi_rowset_debug, nspi_raw_property_tag_candidates,
}`
- `dn_to_mid::parse_dn_to_mid_names`
- `property_values::{
    allocate_nspi_entry_identities, allocate_principal_nspi_identity,
    nspi_get_props_missing_property_value_list, nspi_get_props_property_tags,
    nspi_get_props_property_value_list, nspi_property_tags_response, nspi_resolved_entry_row,
    parse_nspi_get_props_request, NSPI_BOOTSTRAP_PROPERTY_TAGS,
}`
- `pub(in crate::mapi) use property_values::{
    nspi_entry_available_property_tags, nspi_entry_display_type, nspi_entry_id,
    nspi_entry_property_value_list, nspi_known_unsupported_property_tag_name,
    nspi_property_tag_is_supported, nspi_requested_property_tags, principal_address_book_entry,
    principal_minimal_entry_id, write_address_book_tagged_property_value,
    write_large_property_tag_array, NspiValue,
}`
- `property_values::{
    nspi_entry_value, NSPI_ADDITIONAL_REQUESTED_PROPERTY_TAGS, NSPI_SUPPORTED_REQUEST_TYPES,
}`
- `special_tables::NSPI_UNICODE_STRINGS_FLAG`
- `special_tables::{nspi_hierarchy_info_response, nspi_special_table_response}`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)