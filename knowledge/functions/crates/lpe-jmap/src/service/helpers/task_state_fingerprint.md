---
type: Rust Function
title: task_state_fingerprint
resource: crates/lpe-jmap/src/service/helpers.rs#L780-L792
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint
  called_by:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries
---

# Signature

`pub(super) fn task_state_fingerprint(task: &ClientTask) -> String`

# Calls

- [opaque_state_fingerprint](../../../../../../functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint.md)

# Called by

- [object_state_entries](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries.md)