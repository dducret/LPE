---
type: Rust Function
title: contact_state_fingerprint
resource: crates/lpe-jmap/src/service/helpers.rs#L741-L757
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint
  called_by:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries
---

# Signature

`pub(super) fn contact_state_fingerprint(contact: &AccessibleContact) -> String`

# Calls

- [opaque_state_fingerprint](../../../../../../functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint.md)

# Called by

- [object_state_entries](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries.md)