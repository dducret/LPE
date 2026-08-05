---
type: Rust Function
title: nspi_dn_to_mid_request
resource: crates/lpe-exchange/src/tests/mapi_over_http/nspi.rs#L3-L14
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_principal_mid
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_does_not_alias_organization_to_principal
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_preserves_large_array_order_and_duplicates
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_truncated_auxiliary_buffer
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_trailing_bytes_after_auxiliary_buffer
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_bootstrap_requests_handle_stale_cleanup_and_reject_stateful_cookies
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_bootstrap_sequence_sees_only_visible_contacts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_ids_ignore_generic_mapi_identity_cache_collisions
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_query_rows_stays_in_authenticated_tenant
---

# Signature

`pub(super) fn nspi_dn_to_mid_request(names: &[&str]) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts.md)
- [nspi_principal_mid](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_principal_mid.md)
- [mapi_over_http_nspi_dn_to_mid_does_not_alias_organization_to_principal](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_does_not_alias_organization_to_principal.md)
- [mapi_over_http_nspi_dn_to_mid_preserves_large_array_order_and_duplicates](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_preserves_large_array_order_and_duplicates.md)
- [mapi_over_http_nspi_dn_to_mid_rejects_truncated_auxiliary_buffer](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_truncated_auxiliary_buffer.md)
- [mapi_over_http_nspi_dn_to_mid_rejects_trailing_bytes_after_auxiliary_buffer](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_trailing_bytes_after_auxiliary_buffer.md)
- [mapi_over_http_nspi_bootstrap_requests_handle_stale_cleanup_and_reject_stateful_cookies](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_bootstrap_requests_handle_stale_cleanup_and_reject_stateful_cookies.md)
- [mapi_over_http_nspi_bootstrap_sequence_sees_only_visible_contacts](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_bootstrap_sequence_sees_only_visible_contacts.md)
- [mapi_over_http_nspi_ids_ignore_generic_mapi_identity_cache_collisions](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_ids_ignore_generic_mapi_identity_cache_collisions.md)
- [mapi_over_http_query_rows_stays_in_authenticated_tenant](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_query_rows_stays_in_authenticated_tenant.md)