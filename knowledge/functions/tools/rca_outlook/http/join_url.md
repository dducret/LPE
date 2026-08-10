---
type: Python Function
title: join_url
resource: tools/rca_outlook/http.py#L75-L78
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook/ews/ews_call
  - functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness
  - functions/tools/rca_outlook_connectivity_check/check_pox_autodiscover
  - functions/tools/rca_outlook_connectivity_check/check_json_autodiscover
  - functions/tools/rca_outlook_connectivity_check/check_jmap_publication_headers
  - functions/tools/rca_outlook_connectivity_check/check_jmap_session
  - functions/tools/rca_outlook_connectivity_check/check_jmap_email_subject_absent
  - functions/tools/rca_outlook_connectivity_check/check_ews_basic
  - functions/tools/rca_outlook_connectivity_check/check_mapi_ping
  - functions/tools/rca_outlook_connectivity_check/mapi_nspi_bind_cookie
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_resolve_authenticated_mailbox
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest
  - functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture
  - functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_auth
  - functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_mailstore_ping
---

# Signature

`def join_url(base_url: str, path: str) -> str:`

# Called by

- [ews_call](../../../../functions/tools/rca_outlook/ews/ews_call.md)
- [check_mapi_gate1_readiness](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness.md)
- [check_pox_autodiscover](../../../../functions/tools/rca_outlook_connectivity_check/check_pox_autodiscover.md)
- [check_json_autodiscover](../../../../functions/tools/rca_outlook_connectivity_check/check_json_autodiscover.md)
- [check_jmap_publication_headers](../../../../functions/tools/rca_outlook_connectivity_check/check_jmap_publication_headers.md)
- [check_jmap_session](../../../../functions/tools/rca_outlook_connectivity_check/check_jmap_session.md)
- [check_jmap_email_subject_absent](../../../../functions/tools/rca_outlook_connectivity_check/check_jmap_email_subject_absent.md)
- [check_ews_basic](../../../../functions/tools/rca_outlook_connectivity_check/check_ews_basic.md)
- [check_mapi_ping](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_ping.md)
- [mapi_nspi_bind_cookie](../../../../functions/tools/rca_outlook_connectivity_check/mapi_nspi_bind_cookie.md)
- [check_mapi_nspi_address_book](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book.md)
- [check_mapi_nspi_resolve_authenticated_mailbox](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_resolve_authenticated_mailbox.md)
- [check_mapi_emsmdb_sent_message](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message.md)
- [check_mapi_emsmdb_sent_sync_manifest](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest.md)
- [check_mapi_empty_deleted_items_fixture](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture.md)
- [check_rpc_proxy_auth](../../../../functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_auth.md)
- [check_rpc_proxy_mailstore_ping](../../../../functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_mailstore_ping.md)