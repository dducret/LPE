---
type: Rust Method
title: handle_item_operations_fetch
resource: crates/lpe-activesync/src/service/item_operations.rs#L55-L192
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_accesses
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_opaque
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/resolve_collection
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-activesync/src/service/body_preferences/fetch_body_preference
  - functions/crates/lpe-activesync/src/service/sync_helpers/value_to_wbxml
  - functions/crates/lpe-activesync/src/snapshot/email_application_data
  called_by:
  - functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations
---

# Signature

`pub(super) async fn handle_item_operations_fetch( &self, principal: &AuthenticatedPrincipal, fetch: &WbxmlNode, ) -> Result<WbxmlNode>`

# Calls

- [text_value](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [mailbox_accesses](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_accesses.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [with_opaque](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_opaque.md)
- [resolve_collection](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/resolve_collection.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [fetch_body_preference](../../../../../../../functions/crates/lpe-activesync/src/service/body_preferences/fetch_body_preference.md)
- [value_to_wbxml](../../../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/value_to_wbxml.md)
- [email_application_data](../../../../../../../functions/crates/lpe-activesync/src/snapshot/email_application_data.md)

# Called by

- [handle_item_operations](../../../../../../../functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations.md)