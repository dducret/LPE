---
type: Rust Function
title: nspi_bound_headers
resource: crates/lpe-exchange/src/tests/mod.rs#L12600-L12611
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_get_matches_uses_complete_utf16_lookup_value
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_principal_mid
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/assert_nspi_dn_to_mid_request_rejected
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_does_not_alias_organization_to_principal
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_preserves_large_array_order_and_duplicates
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_returns_one_error_for_one_missing_property
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_unknown_current_rec_returns_ordered_errors
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_returns_error_for_missing_null_slot
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_encodes_missing_null_tag_as_error
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_null_get_props_matches_entry_prop_list
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_accepts_rca_octet_stream_resolve_names_probe
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_returns_nspi_and_mailbox_urls
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_resolves_authenticated_mailbox
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_honors_requested_rca_columns
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_falls_back_to_authenticated_mailbox_for_rca
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_resolves_canonical_contact
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_bootstrap_sequence_sees_only_visible_contacts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_returns_microsoft_contact_detail_columns
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_ids_ignore_generic_mapi_identity_cache_collisions
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_ranks_exact_contact_before_partial_account
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_matches_ranks_distribution_list_exact_smtp_first
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_distribution_list_members_are_bounded_to_canonical_rows
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_hidden_authenticated_account_is_not_browsed_but_resolves_self
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_query_rows_stays_in_authenticated_tenant
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_query_rows_honors_requested_count
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_requested_string8_columns_stay_tenant_scoped
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_minimal_ids_use_identity_mapping_not_uuid_prefix
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_returns_no_match_for_unknown_name
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_bootstrap_requests_return_success
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_mutation_requests_return_parseable_disabled_errors
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_dn_to_mid_resolves_outlook_unprefixed_legacy_dn_to_principal
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_dn_to_eph_resolves_outlook_legacy_dn_to_principal
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_dn_to_mid_resolves_connect_display_name_legacy_dn_to_principal
---

# Signature

`async fn nspi_bound_headers(service: &ExchangeService<FakeStore>, request_type: &str) -> HeaderMap`

# Calls

- [mapi_headers](../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)

# Called by

- [mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts.md)
- [mapi_over_http_get_matches_uses_complete_utf16_lookup_value](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_get_matches_uses_complete_utf16_lookup_value.md)
- [nspi_principal_mid](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_principal_mid.md)
- [assert_nspi_dn_to_mid_request_rejected](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/assert_nspi_dn_to_mid_request_rejected.md)
- [mapi_over_http_nspi_dn_to_mid_does_not_alias_organization_to_principal](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_does_not_alias_organization_to_principal.md)
- [mapi_over_http_nspi_dn_to_mid_preserves_large_array_order_and_duplicates](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_preserves_large_array_order_and_duplicates.md)
- [mapi_over_http_nspi_get_props_returns_one_error_for_one_missing_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_returns_one_error_for_one_missing_property.md)
- [mapi_over_http_nspi_get_props_unknown_current_rec_returns_ordered_errors](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_unknown_current_rec_returns_ordered_errors.md)
- [mapi_over_http_nspi_get_props_returns_error_for_missing_null_slot](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_returns_error_for_missing_null_slot.md)
- [mapi_over_http_nspi_get_props_encodes_missing_null_tag_as_error](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_encodes_missing_null_tag_as_error.md)
- [mapi_over_http_nspi_null_get_props_matches_entry_prop_list](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_null_get_props_matches_entry_prop_list.md)
- [mapi_over_http_accepts_rca_octet_stream_resolve_names_probe](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_accepts_rca_octet_stream_resolve_names_probe.md)
- [mapi_over_http_returns_nspi_and_mailbox_urls](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_returns_nspi_and_mailbox_urls.md)
- [mapi_over_http_resolve_names_resolves_authenticated_mailbox](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_resolves_authenticated_mailbox.md)
- [mapi_over_http_resolve_names_honors_requested_rca_columns](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_honors_requested_rca_columns.md)
- [mapi_over_http_resolve_names_falls_back_to_authenticated_mailbox_for_rca](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_falls_back_to_authenticated_mailbox_for_rca.md)
- [mapi_over_http_resolve_names_resolves_canonical_contact](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_resolves_canonical_contact.md)
- [mapi_over_http_nspi_bootstrap_sequence_sees_only_visible_contacts](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_bootstrap_sequence_sees_only_visible_contacts.md)
- [mapi_over_http_nspi_get_props_returns_microsoft_contact_detail_columns](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_returns_microsoft_contact_detail_columns.md)
- [mapi_over_http_nspi_ids_ignore_generic_mapi_identity_cache_collisions](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_ids_ignore_generic_mapi_identity_cache_collisions.md)
- [mapi_over_http_resolve_names_ranks_exact_contact_before_partial_account](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_ranks_exact_contact_before_partial_account.md)
- [mapi_over_http_nspi_get_matches_ranks_distribution_list_exact_smtp_first](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_matches_ranks_distribution_list_exact_smtp_first.md)
- [mapi_over_http_nspi_distribution_list_members_are_bounded_to_canonical_rows](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_distribution_list_members_are_bounded_to_canonical_rows.md)
- [mapi_over_http_hidden_authenticated_account_is_not_browsed_but_resolves_self](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_hidden_authenticated_account_is_not_browsed_but_resolves_self.md)
- [mapi_over_http_query_rows_stays_in_authenticated_tenant](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_query_rows_stays_in_authenticated_tenant.md)
- [mapi_over_http_nspi_query_rows_honors_requested_count](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_query_rows_honors_requested_count.md)
- [mapi_over_http_nspi_requested_string8_columns_stay_tenant_scoped](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_requested_string8_columns_stay_tenant_scoped.md)
- [mapi_over_http_nspi_minimal_ids_use_identity_mapping_not_uuid_prefix](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_minimal_ids_use_identity_mapping_not_uuid_prefix.md)
- [mapi_over_http_resolve_names_returns_no_match_for_unknown_name](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_returns_no_match_for_unknown_name.md)
- [mapi_over_http_nspi_bootstrap_requests_return_success](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_bootstrap_requests_return_success.md)
- [mapi_over_http_nspi_mutation_requests_return_parseable_disabled_errors](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_mutation_requests_return_parseable_disabled_errors.md)
- [mapi_over_http_dn_to_mid_resolves_outlook_unprefixed_legacy_dn_to_principal](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_dn_to_mid_resolves_outlook_unprefixed_legacy_dn_to_principal.md)
- [mapi_over_http_dn_to_eph_resolves_outlook_legacy_dn_to_principal](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_dn_to_eph_resolves_outlook_legacy_dn_to_principal.md)
- [mapi_over_http_dn_to_mid_resolves_connect_display_name_legacy_dn_to_principal](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_dn_to_mid_resolves_connect_display_name_legacy_dn_to_principal.md)