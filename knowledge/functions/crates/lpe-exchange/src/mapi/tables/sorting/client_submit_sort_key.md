---
type: Rust Function
title: client_submit_sort_key
resource: crates/lpe-exchange/src/mapi/tables/sorting.rs#L133-L135
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_mapi_messages
---

# Signature

`fn client_submit_sort_key(email: &JmapEmail) -> &str`

# Called by

- [sort_emails](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails.md)
- [sort_mapi_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_mapi_messages.md)