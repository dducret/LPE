---
type: Rust Function
title: source_key_for_mailbox_role
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L287-L304
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
---

# Signature

`pub(crate) fn source_key_for_mailbox_role(mailbox_id: &Uuid, role: &str) -> Vec<u8>`

# Calls

- [mapped_mapi_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)

# Called by

- [email_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)