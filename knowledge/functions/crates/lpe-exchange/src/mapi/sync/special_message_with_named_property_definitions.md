---
type: Rust Function
title: special_message_with_named_property_definitions
resource: crates/lpe-exchange/src/mapi/sync.rs#L388-L394
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/populate_special_message_named_property_definitions
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
---

# Signature

`fn special_message_with_named_property_definitions( mut object: mapi_mailstore::SpecialMessageSyncFact, snapshot: &MapiMailStoreSnapshot, ) -> mapi_mailstore::SpecialMessageSyncFact`

# Calls

- [populate_special_message_named_property_definitions](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/populate_special_message_named_property_definitions.md)

# Called by

- [fast_transfer_manifest_for_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)