---
type: Python Function
title: content_type
resource: tools/rca_outlook/http.py#L84-L88
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/console/upload_pst_import
  - functions/tools/rca_outlook/ews/ews_call
  - functions/tools/rca_outlook_connectivity_check/check_pox_autodiscover
  - functions/tools/rca_outlook_connectivity_check/check_jmap_session
  - functions/tools/rca_outlook_connectivity_check/check_ews_basic
  - functions/tools/rca_outlook_connectivity_check/check_mapi_ping
  - functions/tools/rca_outlook_connectivity_check/mapi_nspi_bind_cookie
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_resolve_authenticated_mailbox
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest
  - functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_auth
  - functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_mailstore_ping
---

# Signature

`def content_type(headers: dict[str, str]) -> str:`

# Called by

- [upload_pst_import](../../../../functions/crates/lpe-admin-api/src/console/upload_pst_import.md)
- [ews_call](../../../../functions/tools/rca_outlook/ews/ews_call.md)
- [check_pox_autodiscover](../../../../functions/tools/rca_outlook_connectivity_check/check_pox_autodiscover.md)
- [check_jmap_session](../../../../functions/tools/rca_outlook_connectivity_check/check_jmap_session.md)
- [check_ews_basic](../../../../functions/tools/rca_outlook_connectivity_check/check_ews_basic.md)
- [check_mapi_ping](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_ping.md)
- [mapi_nspi_bind_cookie](../../../../functions/tools/rca_outlook_connectivity_check/mapi_nspi_bind_cookie.md)
- [check_mapi_nspi_address_book](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book.md)
- [check_mapi_nspi_resolve_authenticated_mailbox](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_resolve_authenticated_mailbox.md)
- [check_mapi_emsmdb_sent_message](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message.md)
- [check_mapi_emsmdb_sent_sync_manifest](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest.md)
- [check_rpc_proxy_auth](../../../../functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_auth.md)
- [check_rpc_proxy_mailstore_ping](../../../../functions/tools/rca_outlook_connectivity_check/check_rpc_proxy_mailstore_ping.md)