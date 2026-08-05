---
type: Python Function
title: require_ews_no_error
resource: tools/rca_outlook/ews.py#L51-L52
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/require
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_ews_mailbox_access
  - functions/tools/rca_outlook_connectivity_check/check_ews_send_sent
  - functions/tools/rca_outlook_connectivity_check/check_ews_contact_calendar_and_mapi_fixture
  - functions/tools/rca_outlook_connectivity_check/delete_ews_item
  - functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture
---

# Signature

`def require_ews_no_error(name: str, payload: str) -> None:`

# Calls

- [require](../../../../functions/tools/rca_outlook/http/require.md)

# Called by

- [check_ews_mailbox_access](../../../../functions/tools/rca_outlook_connectivity_check/check_ews_mailbox_access.md)
- [check_ews_send_sent](../../../../functions/tools/rca_outlook_connectivity_check/check_ews_send_sent.md)
- [check_ews_contact_calendar_and_mapi_fixture](../../../../functions/tools/rca_outlook_connectivity_check/check_ews_contact_calendar_and_mapi_fixture.md)
- [delete_ews_item](../../../../functions/tools/rca_outlook_connectivity_check/delete_ews_item.md)
- [check_mapi_empty_deleted_items_fixture](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture.md)