---
type: Python Function
title: extract_ews_item_id
resource: tools/rca_outlook/ews.py#L54-L57
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/require
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_ews_send_sent
  - functions/tools/rca_outlook_connectivity_check/check_ews_contact_calendar_and_mapi_fixture
---

# Signature

`def extract_ews_item_id(payload: str, prefix: str, name: str) -> str:`

# Calls

- [require](../../../../functions/tools/rca_outlook/http/require.md)

# Called by

- [check_ews_send_sent](../../../../functions/tools/rca_outlook_connectivity_check/check_ews_send_sent.md)
- [check_ews_contact_calendar_and_mapi_fixture](../../../../functions/tools/rca_outlook_connectivity_check/check_ews_contact_calendar_and_mapi_fixture.md)