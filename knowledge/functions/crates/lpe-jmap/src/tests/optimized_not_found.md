---
type: Rust Function
title: optimized_not_found
resource: crates/lpe-jmap/src/tests.rs#L14884-L14889
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tests/benchmark_mailbox_listing_and_push_paths
---

# Signature

`fn optimized_not_found(mailbox_ids: &HashSet<Uuid>, requested_ids: &[Uuid]) -> usize`

# Called by

- [benchmark_mailbox_listing_and_push_paths](../../../../../functions/crates/lpe-jmap/src/tests/benchmark_mailbox_listing_and_push_paths.md)