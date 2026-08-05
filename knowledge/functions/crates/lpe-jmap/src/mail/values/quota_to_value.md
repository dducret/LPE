---
type: Rust Function
title: quota_to_value
resource: crates/lpe-jmap/src/mail/values.rs#L618-L626
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_quota_get
---

# Signature

`pub(crate) fn quota_to_value(quota: &JmapQuota) -> Value`

# Called by

- [handle_quota_get](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_quota_get.md)