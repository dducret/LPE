---
type: Python Function
title: delete_ews_item
resource: tools/rca_outlook_connectivity_check.py#L930-L953
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/ews/ews_call
  - functions/tools/rca_outlook/ews/require_ews_no_error
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_ews_send_sent
  - functions/tools/rca_outlook_connectivity_check/check_ews_contact_calendar_and_mapi_fixture
---

# Signature

`def delete_ews_item( base_url: str, email: str, password: str, item_id: str, insecure_tls: bool, timeout: int, required: bool, ) -> None:`

# Calls

- [ews_call](../../../functions/tools/rca_outlook/ews/ews_call.md)
- [require_ews_no_error](../../../functions/tools/rca_outlook/ews/require_ews_no_error.md)

# Called by

- [check_ews_send_sent](../../../functions/tools/rca_outlook_connectivity_check/check_ews_send_sent.md)
- [check_ews_contact_calendar_and_mapi_fixture](../../../functions/tools/rca_outlook_connectivity_check/check_ews_contact_calendar_and_mapi_fixture.md)