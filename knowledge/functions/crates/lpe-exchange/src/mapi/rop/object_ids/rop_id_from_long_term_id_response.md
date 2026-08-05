---
type: Rust Function
title: rop_id_from_long_term_id_response
resource: crates/lpe-exchange/src/mapi/rop/object_ids.rs#L22-L41
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/long_term_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/identity/object_id_from_long_term_id_with_replica_guids
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_id_from_long_term_id_response
  - functions/crates/lpe-exchange/src/mapi/rop/tests/id_from_long_term_id_accepts_mailbox_guid_aliases_and_special_stale_guid
---

# Signature

`pub(in crate::mapi) fn rop_id_from_long_term_id_response( request: &RopRequest, replica_guid_aliases: &[[u8; 16]], ) -> Vec<u8>`

# Calls

- [long_term_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/long_term_id.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [object_id_from_long_term_id_with_replica_guids](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_long_term_id_with_replica_guids.md)

# Called by

- [append_id_from_long_term_id_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_id_from_long_term_id_response.md)
- [id_from_long_term_id_accepts_mailbox_guid_aliases_and_special_stale_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/id_from_long_term_id_accepts_mailbox_guid_aliases_and_special_stale_guid.md)