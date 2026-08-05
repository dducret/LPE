---
type: Python Function
title: mapi_rop_buffer
resource: tools/rca_outlook/mapi.py#L41-L47
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest
  - functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture
---

# Signature

`def mapi_rop_buffer(rops: bytes, handles: list[int]) -> bytes:`

# Called by

- [check_mapi_emsmdb_sent_message](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message.md)
- [check_mapi_emsmdb_sent_sync_manifest](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest.md)
- [check_mapi_empty_deleted_items_fixture](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture.md)