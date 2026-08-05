---
type: Rust Function
title: full_query_limit
resource: crates/lpe-jmap/src/mail/values.rs#L16-L18
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/resolve_full_email_query_ids
  - functions/crates/lpe-jmap/src/mail/JmapService/resolve_full_thread_query_ids
---

# Signature

`pub(crate) fn full_query_limit(total: u64) -> u64`

# Called by

- [resolve_full_email_query_ids](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/resolve_full_email_query_ids.md)
- [resolve_full_thread_query_ids](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/resolve_full_thread_query_ids.md)