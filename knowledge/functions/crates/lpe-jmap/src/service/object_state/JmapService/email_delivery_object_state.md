---
type: Rust Method
title: email_delivery_object_state
resource: crates/lpe-jmap/src/service/object_state.rs#L101-L112
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint
  - functions/crates/lpe-jmap/src/state/encode_state
  called_by:
  - functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state
---

# Signature

`pub(crate) async fn email_delivery_object_state(&self, account_id: Uuid) -> Result<String>`

# Calls

- [opaque_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint.md)
- [encode_state](../../../../../../../functions/crates/lpe-jmap/src/state/encode_state.md)

# Called by

- [mail_push_type_state](../../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state.md)