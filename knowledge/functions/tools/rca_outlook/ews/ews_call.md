---
type: Python Function
title: ews_call
resource: tools/rca_outlook/ews.py#L24-L49
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/join_url
  - functions/tools/rca_outlook/ews/ews_envelope
  - functions/tools/rca_outlook/http/basic_auth_header
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/http/content_type
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_ews_mailbox_access
  - functions/tools/rca_outlook_connectivity_check/check_ews_send_sent
  - functions/tools/rca_outlook_connectivity_check/check_ews_contact_calendar_and_mapi_fixture
  - functions/tools/rca_outlook_connectivity_check/delete_ews_item
  - functions/tools/rca_outlook_connectivity_check/require_deleted_ews_item
  - functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture
---

# Signature

`def ews_call( base_url: str, email: str, password: str, operation: str, body: str, insecure_tls: bool, timeout: int, ) -> str:`

# Calls

- [join_url](../../../../functions/tools/rca_outlook/http/join_url.md)
- [ews_envelope](../../../../functions/tools/rca_outlook/ews/ews_envelope.md)
- [basic_auth_header](../../../../functions/tools/rca_outlook/http/basic_auth_header.md)
- [require](../../../../functions/tools/rca_outlook/http/require.md)
- [content_type](../../../../functions/tools/rca_outlook/http/content_type.md)

# Called by

- [check_ews_mailbox_access](../../../../functions/tools/rca_outlook_connectivity_check/check_ews_mailbox_access.md)
- [check_ews_send_sent](../../../../functions/tools/rca_outlook_connectivity_check/check_ews_send_sent.md)
- [check_ews_contact_calendar_and_mapi_fixture](../../../../functions/tools/rca_outlook_connectivity_check/check_ews_contact_calendar_and_mapi_fixture.md)
- [delete_ews_item](../../../../functions/tools/rca_outlook_connectivity_check/delete_ews_item.md)
- [require_deleted_ews_item](../../../../functions/tools/rca_outlook_connectivity_check/require_deleted_ews_item.md)
- [check_mapi_empty_deleted_items_fixture](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture.md)