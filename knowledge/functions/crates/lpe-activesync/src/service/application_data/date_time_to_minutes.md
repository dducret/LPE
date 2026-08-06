---
type: Rust Function
title: date_time_to_minutes
resource: crates/lpe-activesync/src/service/application_data.rs#L285-L309
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-activesync/src/service/application_data/duration_from_datetimes
---

# Signature

`fn date_time_to_minutes(date: &str, time: &str) -> Result<i64>`

# Calls

- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [duration_from_datetimes](../../../../../../functions/crates/lpe-activesync/src/service/application_data/duration_from_datetimes.md)