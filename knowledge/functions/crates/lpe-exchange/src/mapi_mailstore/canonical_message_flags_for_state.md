---
type: Rust Function
title: canonical_message_flags_for_state
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1041-L1059
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/canonical_message_flags
---

# Signature

`pub(crate) fn canonical_message_flags_for_state( unread: bool, draft: bool, has_attachments: bool, ) -> u32`

# Called by

- [canonical_message_flags](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_message_flags.md)