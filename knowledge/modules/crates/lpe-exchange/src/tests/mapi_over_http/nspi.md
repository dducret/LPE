---
type: Rust Module
title: nspi
resource: crates/lpe-exchange/src/tests/mapi_over_http/nspi.rs#L1-L2576
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [nspi_dn_to_mid_request](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_dn_to_mid_request.md)
- [nspi_get_props_request](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_get_props_request.md)
- [nspi_get_props_without_property_tags_request](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_get_props_without_property_tags_request.md)
- [nspi_get_prop_list_request](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_get_prop_list_request.md)
- [nspi_get_prop_list_response_tags](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_get_prop_list_response_tags.md)
- [nspi_get_props_response_tags](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_get_props_response_tags.md)
- [nspi_principal_mid](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_principal_mid.md)
- [assert_nspi_dn_to_mid_request_rejected](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/assert_nspi_dn_to_mid_request_rejected.md)
- [mapi_over_http_nspi_dn_to_mid_does_not_alias_organization_to_principal](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_does_not_alias_organization_to_principal.md)
- [mapi_over_http_nspi_dn_to_mid_preserves_large_array_order_and_duplicates](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_preserves_large_array_order_and_duplicates.md)
- [mapi_over_http_nspi_dn_to_mid_rejects_missing_auxiliary_size_without_names](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_missing_auxiliary_size_without_names.md)
- [mapi_over_http_nspi_dn_to_mid_rejects_truncated_auxiliary_buffer](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_truncated_auxiliary_buffer.md)
- [mapi_over_http_nspi_dn_to_mid_rejects_trailing_bytes_after_auxiliary_buffer](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_trailing_bytes_after_auxiliary_buffer.md)
- [mapi_over_http_nspi_get_props_returns_one_error_for_one_missing_property](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_returns_one_error_for_one_missing_property.md)
- [mapi_over_http_nspi_get_props_unknown_current_rec_returns_ordered_errors](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_unknown_current_rec_returns_ordered_errors.md)
- [mapi_over_http_nspi_get_props_returns_error_for_missing_null_slot](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_returns_error_for_missing_null_slot.md)
- [mapi_over_http_nspi_get_props_encodes_missing_null_tag_as_error](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_encodes_missing_null_tag_as_error.md)
- [mapi_over_http_nspi_null_get_props_matches_entry_prop_list](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_null_get_props_matches_entry_prop_list.md)
- [mapi_over_http_accepts_rca_octet_stream_resolve_names_probe](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_accepts_rca_octet_stream_resolve_names_probe.md)
- [mapi_over_http_ping_refreshes_nspi_session_cookie](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_ping_refreshes_nspi_session_cookie.md)
- [mapi_over_http_bind_creates_nspi_session](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_bind_creates_nspi_session.md)
- [mapi_over_http_bind_reestablishes_nspi_session_cookie](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_bind_reestablishes_nspi_session_cookie.md)
- [mapi_over_http_nspi_operation_requires_bound_session_cookie](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_operation_requires_bound_session_cookie.md)
- [mapi_over_http_nspi_operation_rejects_mismatched_sequence_cookie](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_operation_rejects_mismatched_sequence_cookie.md)
- [mapi_over_http_nspi_bootstrap_requests_handle_stale_cleanup_and_reject_stateful_cookies](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_bootstrap_requests_handle_stale_cleanup_and_reject_stateful_cookies.md)
- [mapi_over_http_returns_nspi_and_mailbox_urls](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_returns_nspi_and_mailbox_urls.md)
- [mapi_over_http_resolve_names_resolves_authenticated_mailbox](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_resolves_authenticated_mailbox.md)
- [mapi_over_http_resolve_names_honors_requested_rca_columns](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_honors_requested_rca_columns.md)
- [mapi_over_http_resolve_names_falls_back_to_authenticated_mailbox_for_rca](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_falls_back_to_authenticated_mailbox_for_rca.md)
- [mapi_over_http_resolve_names_resolves_canonical_contact](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_resolves_canonical_contact.md)
- [mapi_over_http_resolve_names_projects_each_requested_recipient](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_projects_each_requested_recipient.md)
- [mapi_over_http_nspi_bootstrap_sequence_sees_only_visible_contacts](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_bootstrap_sequence_sees_only_visible_contacts.md)
- [mapi_over_http_nspi_get_props_returns_microsoft_contact_detail_columns](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_returns_microsoft_contact_detail_columns.md)
- [mapi_over_http_nspi_ids_ignore_generic_mapi_identity_cache_collisions](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_ids_ignore_generic_mapi_identity_cache_collisions.md)
- [mapi_over_http_resolve_names_ranks_exact_contact_before_partial_account](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_ranks_exact_contact_before_partial_account.md)
- [mapi_over_http_microsoft_oxnspi_hierarchy_and_query_rows_example_round_trips](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_microsoft_oxnspi_hierarchy_and_query_rows_example_round_trips.md)
- [mapi_over_http_nspi_get_matches_ranks_distribution_list_exact_smtp_first](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_matches_ranks_distribution_list_exact_smtp_first.md)
- [mapi_over_http_nspi_distribution_list_members_are_bounded_to_canonical_rows](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_distribution_list_members_are_bounded_to_canonical_rows.md)
- [mapi_over_http_hidden_authenticated_account_is_not_browsed_but_resolves_self](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_hidden_authenticated_account_is_not_browsed_but_resolves_self.md)
- [mapi_over_http_query_rows_stays_in_authenticated_tenant](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_query_rows_stays_in_authenticated_tenant.md)
- [mapi_over_http_nspi_query_rows_honors_requested_count](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_query_rows_honors_requested_count.md)
- [mapi_over_http_nspi_requested_string8_columns_stay_tenant_scoped](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_requested_string8_columns_stay_tenant_scoped.md)
- [mapi_over_http_nspi_minimal_ids_use_identity_mapping_not_uuid_prefix](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_minimal_ids_use_identity_mapping_not_uuid_prefix.md)
- [mapi_over_http_resolve_names_returns_no_match_for_unknown_name](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_returns_no_match_for_unknown_name.md)
- [mapi_over_http_nspi_bootstrap_requests_return_success](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_bootstrap_requests_return_success.md)
- [mapi_over_http_nspi_mutation_requests_return_parseable_disabled_errors](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_mutation_requests_return_parseable_disabled_errors.md)
- [mapi_over_http_dn_to_mid_resolves_outlook_unprefixed_legacy_dn_to_principal](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_dn_to_mid_resolves_outlook_unprefixed_legacy_dn_to_principal.md)
- [mapi_over_http_dn_to_eph_resolves_outlook_legacy_dn_to_principal](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_dn_to_eph_resolves_outlook_legacy_dn_to_principal.md)
- [mapi_over_http_dn_to_mid_resolves_connect_display_name_legacy_dn_to_principal](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_dn_to_mid_resolves_connect_display_name_legacy_dn_to_principal.md)
- [mapi_over_http_unbind_consumes_nspi_session](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_unbind_consumes_nspi_session.md)

# Imports

- `super::*`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)