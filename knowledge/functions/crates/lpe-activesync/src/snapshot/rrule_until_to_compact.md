---
type: Rust Function
title: rrule_until_to_compact
resource: crates/lpe-activesync/src/snapshot.rs#L439-L446
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-activesync/src/snapshot/recurrence_application_data
---

# Signature

`fn rrule_until_to_compact(value: &str) -> String`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [recurrence_application_data](../../../../../functions/crates/lpe-activesync/src/snapshot/recurrence_application_data.md)