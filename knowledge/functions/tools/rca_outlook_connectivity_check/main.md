---
type: Python Function
title: main
resource: tools/rca_outlook_connectivity_check.py#L1330-L1450
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook_connectivity_check/check_pox_autodiscover
  - functions/tools/rca_outlook_connectivity_check/check_json_autodiscover
  - functions/tools/rca_outlook_connectivity_check/check_jmap_publication_headers
  - functions/tools/rca_outlook_connectivity_check/check_jmap_session
  - functions/tools/rca_outlook_connectivity_check/check_ews_basic
  - functions/tools/rca_outlook_connectivity_check/check_ews_mailbox_access
  - functions/tools/rca_outlook_connectivity_check/check_ews_send_sent
  - functions/tools/rca_outlook_connectivity_check/check_ews_contact_calendar_and_mapi_fixture
  - functions/tools/rca_outlook_connectivity_check/check_mapi_ping
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_bind_octet_stream
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_resolve_authenticated_mailbox
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book
  - functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture
  - functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_auth
  - functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_mailstore_ping
---

# Signature

`def main() -> int:`

# Calls

- [require](../../../functions/tools/rca_outlook/http/require.md)
- [check_pox_autodiscover](../../../functions/tools/rca_outlook_connectivity_check/check_pox_autodiscover.md)
- [check_json_autodiscover](../../../functions/tools/rca_outlook_connectivity_check/check_json_autodiscover.md)
- [check_jmap_publication_headers](../../../functions/tools/rca_outlook_connectivity_check/check_jmap_publication_headers.md)
- [check_jmap_session](../../../functions/tools/rca_outlook_connectivity_check/check_jmap_session.md)
- [check_ews_basic](../../../functions/tools/rca_outlook_connectivity_check/check_ews_basic.md)
- [check_ews_mailbox_access](../../../functions/tools/rca_outlook_connectivity_check/check_ews_mailbox_access.md)
- [check_ews_send_sent](../../../functions/tools/rca_outlook_connectivity_check/check_ews_send_sent.md)
- [check_ews_contact_calendar_and_mapi_fixture](../../../functions/tools/rca_outlook_connectivity_check/check_ews_contact_calendar_and_mapi_fixture.md)
- [check_mapi_ping](../../../functions/tools/rca_outlook_connectivity_check/check_mapi_ping.md)
- [check_mapi_nspi_bind_octet_stream](../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_bind_octet_stream.md)
- [check_mapi_nspi_resolve_authenticated_mailbox](../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_resolve_authenticated_mailbox.md)
- [check_mapi_nspi_address_book](../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book.md)
- [check_mapi_empty_deleted_items_fixture](../../../functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture.md)
- [check_rpc_proxy_auth](../../../functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_auth.md)
- [check_rpc_proxy_mailstore_ping](../../../functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_mailstore_ping.md)