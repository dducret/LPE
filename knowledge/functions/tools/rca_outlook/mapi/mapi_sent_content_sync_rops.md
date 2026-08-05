---
type: Python Function
title: mapi_sent_content_sync_rops
resource: tools/rca_outlook/mapi.py#L62-L75
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest
---

# Signature

`def mapi_sent_content_sync_rops(buffer_size: int = 4096) -> bytes:`

# Called by

- [check_mapi_emsmdb_sent_sync_manifest](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_sync_manifest.md)