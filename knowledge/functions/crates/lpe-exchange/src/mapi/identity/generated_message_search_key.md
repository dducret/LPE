---
type: Rust Function
title: generated_message_search_key
resource: crates/lpe-exchange/src/mapi/identity.rs#L924-L929
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_search_key
---

# Signature

`pub(crate) fn generated_message_search_key(canonical_id: &Uuid) -> Vec<u8>`

# Called by

- [associated_config_fai_content_sync_emits_valid_property_definitions](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)
- [special_message_search_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_search_key.md)