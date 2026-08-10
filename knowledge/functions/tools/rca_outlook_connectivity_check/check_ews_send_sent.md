---
type: Python Function
title: check_ews_send_sent
resource: tools/rca_outlook_connectivity_check.py#L712-L803
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/ews/ews_call
  - functions/tools/rca_outlook/ews/require_ews_no_error
  - functions/tools/rca_outlook/ews/extract_ews_item_id
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest
  - functions/tools/rca_outlook_connectivity_check/delete_ews_item
  called_by:
  - functions/tools/rca_outlook_connectivity_check/main
---

# Signature

`def check_ews_send_sent( base_url: str, email: str, password: str, recipient: str, insecure_tls: bool, timeout: int, check_mapi: bool, ) -> None:`

# Calls

- [ews_call](../../../functions/tools/rca_outlook/ews/ews_call.md)
- [require_ews_no_error](../../../functions/tools/rca_outlook/ews/require_ews_no_error.md)
- [extract_ews_item_id](../../../functions/tools/rca_outlook/ews/extract_ews_item_id.md)
- [require](../../../functions/tools/rca_outlook/http/require.md)
- [check_mapi_emsmdb_sent_message](../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message.md)
- [check_mapi_emsmdb_sent_sync_manifest](../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest.md)
- [delete_ews_item](../../../functions/tools/rca_outlook_connectivity_check/delete_ews_item.md)

# Called by

- [main](../../../functions/tools/rca_outlook_connectivity_check/main.md)