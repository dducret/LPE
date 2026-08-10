---
type: Python Function
title: mapi_execute_response_rops
resource: tools/rca_outlook/mapi.py#L226-L237
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/mapi/le_u32
  called_by:
  - functions/tools/rca_outlook_connectivity_check/mapi_gate1_execute_response_rops
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest
  - functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture
---

# Signature

`def mapi_execute_response_rops(payload: bytes, label: str) -> bytes:`

# Calls

- [require](../../../../functions/tools/rca_outlook/http/require.md)
- [le_u32](../../../../functions/tools/rca_outlook/mapi/le_u32.md)

# Called by

- [mapi_gate1_execute_response_rops](../../../../functions/tools/rca_outlook_connectivity_check/mapi_gate1_execute_response_rops.md)
- [check_mapi_emsmdb_sent_message](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message.md)
- [check_mapi_emsmdb_sent_sync_manifest](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest.md)
- [check_mapi_empty_deleted_items_fixture](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture.md)