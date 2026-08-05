---
type: Rust Function
title: identity_state_fingerprint
resource: crates/lpe-jmap/src/service/helpers.rs#L691-L701
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint
  called_by:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/identity_object_state_entries
---

# Signature

`pub(super) fn identity_state_fingerprint(identity: &SenderIdentity) -> String`

# Calls

- [opaque_state_fingerprint](../../../../../../functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint.md)

# Called by

- [identity_object_state_entries](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/identity_object_state_entries.md)