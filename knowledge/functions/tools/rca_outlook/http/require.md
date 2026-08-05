---
type: Python Function
title: require
resource: tools/rca_outlook/http.py#L80-L82
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook/ews/ews_call
  - functions/tools/rca_outlook/ews/require_ews_no_error
  - functions/tools/rca_outlook/ews/extract_ews_item_id
  - functions/tools/rca_outlook/http/require_guid_counter_header
  - functions/tools/rca_outlook/mapi/mapi_http_binary_payload
  - functions/tools/rca_outlook/mapi/mapi_execute_response_rops
  - functions/tools/rca_outlook/mapi/le_u32
  - functions/tools/rca_outlook/mapi/nspi_first_minimal_id
  - functions/tools/rca_outlook/mapi/assert_nspi_common_success
  - functions/tools/rca_outlook/mapi/assert_nspi_resolve_names_payload
  - functions/tools/rca_outlook/mapi/assert_nspi_get_matches_payload
  - functions/tools/rca_outlook/mapi/assert_nspi_query_rows_payload
  - functions/tools/rca_outlook/mapi/assert_nspi_get_props_payload
  - functions/tools/rca_outlook/mapi/assert_nspi_fixture_payload
  - functions/tools/rca_outlook_connectivity_check/check_pox_autodiscover
  - functions/tools/rca_outlook_connectivity_check/check_json_autodiscover
  - functions/tools/rca_outlook_connectivity_check/check_jmap_publication_headers
  - functions/tools/rca_outlook_connectivity_check/check_jmap_session
  - functions/tools/rca_outlook_connectivity_check/check_jmap_email_subject_absent
  - functions/tools/rca_outlook_connectivity_check/check_ews_basic
  - functions/tools/rca_outlook_connectivity_check/check_ews_mailbox_access
  - functions/tools/rca_outlook_connectivity_check/check_ews_send_sent
  - functions/tools/rca_outlook_connectivity_check/check_ews_contact_calendar_and_mapi_fixture
  - functions/tools/rca_outlook_connectivity_check/require_deleted_ews_item
  - functions/tools/rca_outlook_connectivity_check/check_mapi_ping
  - functions/tools/rca_outlook_connectivity_check/mapi_nspi_bind_cookie
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_resolve_authenticated_mailbox
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest
  - functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture
  - functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_auth
  - functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_mailstore_ping
  - functions/tools/rca_outlook_connectivity_check/main
---

# Signature

`def require(condition: bool, message: str) -> None:`

# Called by

- [ews_call](../../../../functions/tools/rca_outlook/ews/ews_call.md)
- [require_ews_no_error](../../../../functions/tools/rca_outlook/ews/require_ews_no_error.md)
- [extract_ews_item_id](../../../../functions/tools/rca_outlook/ews/extract_ews_item_id.md)
- [require_guid_counter_header](../../../../functions/tools/rca_outlook/http/require_guid_counter_header.md)
- [mapi_http_binary_payload](../../../../functions/tools/rca_outlook/mapi/mapi_http_binary_payload.md)
- [mapi_execute_response_rops](../../../../functions/tools/rca_outlook/mapi/mapi_execute_response_rops.md)
- [le_u32](../../../../functions/tools/rca_outlook/mapi/le_u32.md)
- [nspi_first_minimal_id](../../../../functions/tools/rca_outlook/mapi/nspi_first_minimal_id.md)
- [assert_nspi_common_success](../../../../functions/tools/rca_outlook/mapi/assert_nspi_common_success.md)
- [assert_nspi_resolve_names_payload](../../../../functions/tools/rca_outlook/mapi/assert_nspi_resolve_names_payload.md)
- [assert_nspi_get_matches_payload](../../../../functions/tools/rca_outlook/mapi/assert_nspi_get_matches_payload.md)
- [assert_nspi_query_rows_payload](../../../../functions/tools/rca_outlook/mapi/assert_nspi_query_rows_payload.md)
- [assert_nspi_get_props_payload](../../../../functions/tools/rca_outlook/mapi/assert_nspi_get_props_payload.md)
- [assert_nspi_fixture_payload](../../../../functions/tools/rca_outlook/mapi/assert_nspi_fixture_payload.md)
- [check_pox_autodiscover](../../../../functions/tools/rca_outlook_connectivity_check/check_pox_autodiscover.md)
- [check_json_autodiscover](../../../../functions/tools/rca_outlook_connectivity_check/check_json_autodiscover.md)
- [check_jmap_publication_headers](../../../../functions/tools/rca_outlook_connectivity_check/check_jmap_publication_headers.md)
- [check_jmap_session](../../../../functions/tools/rca_outlook_connectivity_check/check_jmap_session.md)
- [check_jmap_email_subject_absent](../../../../functions/tools/rca_outlook_connectivity_check/check_jmap_email_subject_absent.md)
- [check_ews_basic](../../../../functions/tools/rca_outlook_connectivity_check/check_ews_basic.md)
- [check_ews_mailbox_access](../../../../functions/tools/rca_outlook_connectivity_check/check_ews_mailbox_access.md)
- [check_ews_send_sent](../../../../functions/tools/rca_outlook_connectivity_check/check_ews_send_sent.md)
- [check_ews_contact_calendar_and_mapi_fixture](../../../../functions/tools/rca_outlook_connectivity_check/check_ews_contact_calendar_and_mapi_fixture.md)
- [require_deleted_ews_item](../../../../functions/tools/rca_outlook_connectivity_check/require_deleted_ews_item.md)
- [check_mapi_ping](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_ping.md)
- [mapi_nspi_bind_cookie](../../../../functions/tools/rca_outlook_connectivity_check/mapi_nspi_bind_cookie.md)
- [check_mapi_nspi_address_book](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book.md)
- [check_mapi_nspi_resolve_authenticated_mailbox](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_resolve_authenticated_mailbox.md)
- [check_mapi_emsmdb_sent_message](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message.md)
- [check_mapi_emsmdb_sent_sync_manifest](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest.md)
- [check_mapi_empty_deleted_items_fixture](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture.md)
- [check_rpc_proxy_auth](../../../../functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_auth.md)
- [check_rpc_proxy_mailstore_ping](../../../../functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_mailstore_ping.md)
- [main](../../../../functions/tools/rca_outlook_connectivity_check/main.md)