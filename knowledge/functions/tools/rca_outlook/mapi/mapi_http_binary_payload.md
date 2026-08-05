---
type: Python Function
title: mapi_http_binary_payload
resource: tools/rca_outlook/mapi.py#L129-L132
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/require
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_resolve_authenticated_mailbox
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest
  - functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture
---

# Signature

`def mapi_http_binary_payload(body: bytes) -> bytes:`

# Calls

- [require](../../../../functions/tools/rca_outlook/http/require.md)

# Called by

- [check_mapi_nspi_address_book](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book.md)
- [check_mapi_nspi_resolve_authenticated_mailbox](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_resolve_authenticated_mailbox.md)
- [check_mapi_emsmdb_sent_message](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message.md)
- [check_mapi_emsmdb_sent_sync_manifest](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest.md)
- [check_mapi_empty_deleted_items_fixture](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture.md)