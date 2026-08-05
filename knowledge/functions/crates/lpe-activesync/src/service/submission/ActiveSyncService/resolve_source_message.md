---
type: Rust Method
title: resolve_source_message
resource: crates/lpe-activesync/src/service/submission.rs#L233-L259
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_accesses
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose
---

# Signature

`async fn resolve_source_message( &self, principal: &AuthenticatedPrincipal, request: &WbxmlNode, ) -> Result<(lpe_storage::MailboxAccountAccess, lpe_storage::JmapEmail)>`

# Calls

- [text_value](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [mailbox_accesses](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_accesses.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [handle_smart_compose](../../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose.md)