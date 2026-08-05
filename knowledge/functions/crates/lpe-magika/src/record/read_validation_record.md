---
type: Rust Function
title: read_validation_record
resource: crates/lpe-magika/src/record.rs#L31-L36
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/record/validation_sidecar_path
  - functions/crates/lpe-core/src/sieve/context
  called_by:
  - functions/crates/lpe-storage/src/pst/validate_pst_import_path
---

# Signature

`pub fn read_validation_record(path: &Path) -> Result<PersistedValidationRecord>`

# Calls

- [validation_sidecar_path](../../../../../functions/crates/lpe-magika/src/record/validation_sidecar_path.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)

# Called by

- [validate_pst_import_path](../../../../../functions/crates/lpe-storage/src/pst/validate_pst_import_path.md)