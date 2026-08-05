---
type: Rust Function
title: share_audit
resource: crates/lpe-jmap/src/service/helpers.rs#L139-L149
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_share_set
---

# Signature

`pub(super) fn share_audit( account: &AuthenticatedAccount, action: &str, subject: &str, ) -> AuditEntryInput`

# Called by

- [handle_share_set](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_share_set.md)