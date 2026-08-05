---
type: Rust Module
title: record
resource: crates/lpe-magika/src/record.rs#L1-L49
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-context-result
  - external/std-fs-path-path-pathbuf-time-systemtime-unix-epoch
  - external/crate-types-persistedvalidationrecord-validationoutcome-validationrequest
  member_of:
  - packages/crates/lpe-magika
---

# Contains

- [write_validation_record](../../../../functions/crates/lpe-magika/src/record/write_validation_record.md)
- [read_validation_record](../../../../functions/crates/lpe-magika/src/record/read_validation_record.md)
- [validation_sidecar_path](../../../../functions/crates/lpe-magika/src/record/validation_sidecar_path.md)
- [unix_timestamp](../../../../functions/crates/lpe-magika/src/record/unix_timestamp.md)

# Imports

- `anyhow::{Context, Result}`
- `std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
}`
- `crate::types::{PersistedValidationRecord, ValidationOutcome, ValidationRequest}`

# Member of

- [lpe-magika](../../../../packages/crates/lpe-magika.md)