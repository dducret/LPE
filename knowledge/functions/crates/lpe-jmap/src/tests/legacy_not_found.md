---
type: Rust Function
title: legacy_not_found
resource: crates/lpe-jmap/src/tests.rs#L15417-L15422
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tests/benchmark_mailbox_listing_and_push_paths
---

# Signature

`fn legacy_not_found(mailboxes: &[JmapMailbox], requested_ids: &[Uuid]) -> usize`

# Called by

- [benchmark_mailbox_listing_and_push_paths](../../../../../functions/crates/lpe-jmap/src/tests/benchmark_mailbox_listing_and_push_paths.md)