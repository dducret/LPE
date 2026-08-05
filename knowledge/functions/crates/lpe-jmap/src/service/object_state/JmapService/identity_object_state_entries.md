---
type: Rust Method
title: identity_object_state_entries
resource: crates/lpe-jmap/src/service/object_state.rs#L153-L169
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/identity_state_fingerprint
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_changes
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/identity_object_state
---

# Signature

`pub(crate) async fn identity_object_state_entries( &self, principal_account_id: Uuid, target_account_id: Uuid, ) -> Result<Vec<StateEntry>>`

# Calls

- [identity_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/identity_state_fingerprint.md)

# Called by

- [handle_identity_changes](../../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_changes.md)
- [identity_object_state](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/identity_object_state.md)