---
type: Rust Method
title: identity_object_state
resource: crates/lpe-jmap/src/service/object_state.rs#L142-L151
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/identity_object_state_entries
  - functions/crates/lpe-jmap/src/state/encode_state
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_get
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state
  - functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state
---

# Signature

`pub(crate) async fn identity_object_state( &self, principal_account_id: Uuid, target_account_id: Uuid, ) -> Result<String>`

# Calls

- [identity_object_state_entries](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/identity_object_state_entries.md)
- [encode_state](../../../../../../../functions/crates/lpe-jmap/src/state/encode_state.md)

# Called by

- [handle_identity_get](../../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_get.md)
- [canonical_object_state](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state.md)
- [mail_push_type_state](../../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state.md)