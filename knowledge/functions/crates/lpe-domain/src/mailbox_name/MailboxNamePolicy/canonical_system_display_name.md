---
type: Rust Method
title: canonical_system_display_name
resource: crates/lpe-domain/src/mailbox_name.rs#L213-L230
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/util/canonical_system_mailbox_display_name
---

# Signature

`pub fn canonical_system_display_name(role: &str) -> Option<&'static str>`

# Called by

- [canonical_system_mailbox_display_name](../../../../../../functions/crates/lpe-storage/src/util/canonical_system_mailbox_display_name.md)