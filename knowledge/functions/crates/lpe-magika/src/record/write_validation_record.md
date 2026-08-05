---
type: Rust Function
title: write_validation_record
resource: crates/lpe-magika/src/record.rs#L10-L29
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/record/validation_sidecar_path
  called_by:
  - functions/crates/lpe-admin-api/src/pst/validate_uploaded_pst_file_with_validator
  - functions/crates/lpe-storage/src/shared/pst_processing_requires_prior_validation_record
---

# Signature

`pub fn write_validation_record( path: &Path, request: &ValidationRequest, outcome: &ValidationOutcome, file_size: u64, ) -> Result<PathBuf>`

# Calls

- [validation_sidecar_path](../../../../../functions/crates/lpe-magika/src/record/validation_sidecar_path.md)

# Called by

- [validate_uploaded_pst_file_with_validator](../../../../../functions/crates/lpe-admin-api/src/pst/validate_uploaded_pst_file_with_validator.md)
- [pst_processing_requires_prior_validation_record](../../../../../functions/crates/lpe-storage/src/shared/pst_processing_requires_prior_validation_record.md)