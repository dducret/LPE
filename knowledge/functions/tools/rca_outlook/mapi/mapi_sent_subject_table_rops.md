---
type: Python Function
title: mapi_sent_subject_table_rops
resource: tools/rca_outlook/mapi.py#L141-L152
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message
---

# Signature

`def mapi_sent_subject_table_rops(row_count: int = 20) -> bytes:`

# Called by

- [check_mapi_emsmdb_sent_message](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_emsmdb_sent_message.md)