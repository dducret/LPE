---
type: Rust Method
title: with_policy_enforcement
resource: crates/lpe-activesync/src/service.rs#L84-L89
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/from_env
  - functions/crates/lpe-activesync/src/tests/enforced_mode_validates_later_command_policy_key
---

# Signature

`pub fn with_policy_enforcement(store: S) -> Self`

# Called by

- [from_env](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/from_env.md)
- [enforced_mode_validates_later_command_policy_key](../../../../../../functions/crates/lpe-activesync/src/tests/enforced_mode_validates_later_command_policy_key.md)