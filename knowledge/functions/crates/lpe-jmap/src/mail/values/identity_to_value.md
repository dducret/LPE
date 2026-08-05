---
type: Rust Function
title: identity_to_value
resource: crates/lpe-jmap/src/mail/values.rs#L541-L582
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/insert_if
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_get
---

# Signature

`pub(crate) fn identity_to_value(identity: &SenderIdentity, properties: &HashSet<String>) -> Value`

# Calls

- [insert_if](../../../../../../functions/crates/lpe-jmap/src/convert/insert_if.md)

# Called by

- [handle_identity_get](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_get.md)