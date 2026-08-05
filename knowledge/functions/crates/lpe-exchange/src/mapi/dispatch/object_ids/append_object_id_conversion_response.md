---
type: Rust Function
title: append_object_id_conversion_response
resource: crates/lpe-exchange/src/mapi/dispatch/object_ids.rs#L203-L220
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_long_term_id_from_id_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_id_from_long_term_id_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) fn append_object_id_conversion_response( principal: &AccountPrincipal, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [append_long_term_id_from_id_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_long_term_id_from_id_response.md)
- [append_id_from_long_term_id_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_id_from_long_term_id_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)