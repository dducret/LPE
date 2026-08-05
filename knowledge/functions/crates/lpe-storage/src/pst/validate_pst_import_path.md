---
type: Rust Function
title: validate_pst_import_path
resource: crates/lpe-storage/src/pst.rs#L523-L567
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/record/read_validation_record
  - functions/crates/lpe-magika/src/validator/Validator/validate_path
  called_by:
  - functions/crates/lpe-storage/src/pst/Storage/import_mailbox_from_pst
  - functions/crates/lpe-storage/src/shared/pst_processing_requires_prior_validation_record
---

# Signature

`pub(crate) fn validate_pst_import_path(path: &Path) -> Result<()>`

# Calls

- [read_validation_record](../../../../../functions/crates/lpe-magika/src/record/read_validation_record.md)
- [validate_path](../../../../../functions/crates/lpe-magika/src/validator/Validator/validate_path.md)

# Called by

- [import_mailbox_from_pst](../../../../../functions/crates/lpe-storage/src/pst/Storage/import_mailbox_from_pst.md)
- [pst_processing_requires_prior_validation_record](../../../../../functions/crates/lpe-storage/src/shared/pst_processing_requires_prior_validation_record.md)