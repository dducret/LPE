---
type: Rust Module
title: rpc_proxy_endpoints
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L1-L1212
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-result
  - external/lpe-domain-normalization
  - external/lpe-magika-detector-validator
  - external/lpe-mail-auth-accountprincipal
  - external/tracing-warn
  - external/uuid-uuid
  - external/crate-mapi-store-exchangeaddressbookdirectorykind-exchangeaddressbookentry-exchangeaddressbookentrydetails-exchangeaddressbookentrykind-exchangestore
  - external/super-rpc-proxy-codec-push-le-u32-read-le-u32-rpc-proxy-push-ndr-ascii-string-rpc-proxy-push-ndr-byte-array-rpc-proxy-push-ndr-utf16-string
  - external/super-rpc-proxy-dce-rpc-proxy-dce-fault-response-rpc-proxy-dce-response-rpc-proxy-dce-fault-protocol-error
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [rpc_proxy_nspi_response_for_opnum](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_response_for_opnum.md)
- [rpc_proxy_nspi_response_for_opnum_with_store](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_response_for_opnum_with_store.md)
- [rpc_proxy_mgmt_inq_stats_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_mgmt_inq_stats_response.md)
- [rpc_proxy_emsmdb_connect_ex_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response.md)
- [rpc_proxy_emsmdb_connect_ex_response_for_principal](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response_for_principal.md)
- [rpc_proxy_emsmdb_connect_ex_response_with_context](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response_with_context.md)
- [rpc_proxy_emsmdb_rpc_ext2_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response.md)
- [rpc_proxy_emsmdb_rpc_ext2_response_for_principal](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_for_principal.md)
- [rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer.md)
- [rpc_proxy_emsmdb_disconnect_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_disconnect_response.md)
- [rpc_proxy_push_emsmdb_context_handle](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_emsmdb_context_handle.md)
- [rpc_proxy_rpc_header_ext_payload](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rpc_header_ext_payload.md)
- [rpc_proxy_emsmdb_rpc_ext2_request](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_request.md)
- [rpc_proxy_rfri_get_new_dsa_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response.md)
- [rpc_proxy_rfri_get_new_dsa_response_for_principal](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response_for_principal.md)
- [rpc_proxy_rfri_get_fqdn_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response.md)
- [rpc_proxy_rfri_get_fqdn_response_for_principal](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response_for_principal.md)
- [rpc_proxy_referral_server_name](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_referral_server_name.md)
- [rpc_proxy_referral_server_name_for_principal](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_referral_server_name_for_principal.md)
- [rpc_proxy_nspi_bind_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_bind_response.md)
- [rpc_proxy_nspi_unbind_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_unbind_response.md)
- [rpc_proxy_nspi_update_stat_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_update_stat_response.md)
- [rpc_proxy_nspi_query_rows_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_query_rows_response.md)
- [rpc_proxy_nspi_query_rows_response_for_principal](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_query_rows_response_for_principal.md)
- [rpc_proxy_nspi_get_matches_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response.md)
- [rpc_proxy_nspi_get_matches_response_for_principal](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response_for_principal.md)
- [rpc_proxy_nspi_resort_restriction_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resort_restriction_response.md)
- [rpc_proxy_nspi_minimal_ids_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_minimal_ids_response.md)
- [rpc_proxy_nspi_property_tags_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_property_tags_response.md)
- [rpc_proxy_nspi_get_names_from_ids_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_names_from_ids_response.md)
- [rpc_proxy_nspi_get_props_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response.md)
- [rpc_proxy_nspi_get_props_response_for_principal](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response_for_principal.md)
- [rpc_proxy_nspi_compare_mids_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_compare_mids_response.md)
- [rpc_proxy_nspi_get_special_table_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_special_table_response.md)
- [rpc_proxy_nspi_resolve_names_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response.md)
- [rpc_proxy_nspi_resolve_names_response_for_principal](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal.md)
- [RpcProxyNspiValue](../../../../../classes/crates/lpe-exchange/src/service/rpc_proxy_endpoints/RpcProxyNspiValue.md)
- [rpc_proxy_nspi_requested_property_tags](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_property_tags.md)
- [rpc_proxy_nspi_known_property_tags](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_known_property_tags.md)
- [rpc_proxy_nspi_requested_resolve_property_tags](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_resolve_property_tags.md)
- [rpc_proxy_nspi_requested_smtp_address](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_smtp_address.md)
- [rpc_proxy_display_name_for_smtp_address](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_display_name_for_smtp_address.md)
- [rpc_proxy_push_property_tag_array](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_property_tag_array.md)
- [rpc_proxy_push_stat](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_stat.md)
- [rpc_proxy_nspi_row_values](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_row_values.md)
- [rpc_proxy_address_book_entries](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_address_book_entries.md)
- [rpc_proxy_principal_address_book_entry](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_principal_address_book_entry.md)
- [rpc_proxy_nspi_row_values_for_entry](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_row_values_for_entry.md)
- [rpc_proxy_nspi_entry_id](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_id.md)
- [rpc_proxy_nspi_entry_legacy_name](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_legacy_name.md)
- [rpc_proxy_filter_nspi_entries](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_filter_nspi_entries.md)
- [rpc_proxy_requested_nspi_entry](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_requested_nspi_entry.md)
- [rpc_proxy_match_nspi_entry](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_match_nspi_entry.md)
- [rpc_proxy_nspi_entry_is_principal](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_is_principal.md)
- [rpc_proxy_nspi_principal_matches](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_principal_matches.md)
- [rpc_proxy_nspi_entry_exact_match](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_exact_match.md)
- [rpc_proxy_nspi_entry_matches](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_matches.md)
- [rpc_proxy_nspi_requested_mids](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_mids.md)
- [rpc_proxy_nspi_lookup_values](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_lookup_values.md)
- [rpc_proxy_nspi_ascii_lookup_values](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_ascii_lookup_values.md)
- [rpc_proxy_nspi_utf16_lookup_values](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_utf16_lookup_values.md)
- [rpc_proxy_normalize_nspi_lookup_value](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_normalize_nspi_lookup_value.md)
- [rpc_proxy_push_rowset_pointer](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_rowset_pointer.md)
- [rpc_proxy_push_property_row](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_property_row.md)

# Imports

- `anyhow::{anyhow, Result}`
- `lpe_domain::normalization`
- `lpe_magika::{Detector, Validator}`
- `lpe_mail_auth::AccountPrincipal`
- `tracing::warn`
- `uuid::Uuid`
- `crate::{
    mapi,
    store::{
        ExchangeAddressBookDirectoryKind, ExchangeAddressBookEntry,
        ExchangeAddressBookEntryDetails, ExchangeAddressBookEntryKind, ExchangeStore,
    },
}`
- `super::rpc_proxy_codec::{
    push_le_u32, read_le_u32, rpc_proxy_push_ndr_ascii_string, rpc_proxy_push_ndr_byte_array,
    rpc_proxy_push_ndr_utf16_string,
}`
- `super::rpc_proxy_dce::{
    rpc_proxy_dce_fault_response, rpc_proxy_dce_response, RPC_PROXY_DCE_FAULT_PROTOCOL_ERROR,
}`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)