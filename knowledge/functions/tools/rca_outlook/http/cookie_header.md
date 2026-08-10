---
type: Python Function
title: cookie_header
resource: tools/rca_outlook/http.py#L96-L101
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook/http/update_cookie_header
  - functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness
  - functions/tools/rca_outlook_connectivity_check/check_mapi_ping
  - functions/tools/rca_outlook_connectivity_check/mapi_nspi_bind_cookie
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest
  - functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture
---

# Signature

`def cookie_header(response: HttpResponse) -> str:`

# Called by

- [update_cookie_header](../../../../functions/tools/rca_outlook/http/update_cookie_header.md)
- [check_mapi_gate1_readiness](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness.md)
- [check_mapi_ping](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_ping.md)
- [mapi_nspi_bind_cookie](../../../../functions/tools/rca_outlook_connectivity_check/mapi_nspi_bind_cookie.md)
- [check_mapi_emsmdb_sent_message](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message.md)
- [check_mapi_emsmdb_sent_sync_manifest](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest.md)
- [check_mapi_empty_deleted_items_fixture](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture.md)