---
type: Rust Function
title: media_type_label
resource: crates/lpe-storage/src/attachments.rs#L1231-L1249
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-storage/src/attachments/attachment_kind
---

# Signature

`fn media_type_label(media_type: &str) -> Option<String>`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [attachment_kind](../../../../../functions/crates/lpe-storage/src/attachments/attachment_kind.md)