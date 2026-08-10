---
type: Python Function
title: require_deleted_ews_item
resource: tools/rca_outlook_connectivity_check.py#L956-L981
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/ews/ews_call
  - functions/tools/rca_outlook/http/require
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_ews_contact_calendar_and_mapi_fixture
---

# Signature

`def require_deleted_ews_item( base_url: str, email: str, password: str, item_id: str | None, label: str, insecure_tls: bool, timeout: int, ) -> None:`

# Calls

- [ews_call](../../../functions/tools/rca_outlook/ews/ews_call.md)
- [require](../../../functions/tools/rca_outlook/http/require.md)

# Called by

- [check_ews_contact_calendar_and_mapi_fixture](../../../functions/tools/rca_outlook_connectivity_check/check_ews_contact_calendar_and_mapi_fixture.md)