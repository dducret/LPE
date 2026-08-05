---
type: Rust Function
title: validate_uploaded_pst_file_with_validator
resource: crates/lpe-admin-api/src/pst.rs#L34-L57
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/validator/Validator/validate_path
  - functions/crates/lpe-magika/src/record/write_validation_record
  called_by:
  - functions/crates/lpe-admin-api/src/app/pst_upload_validation_accepts_valid_pst_like_file
  - functions/crates/lpe-admin-api/src/app/pst_upload_validation_rejects_extension_and_type_mismatch
  - functions/crates/lpe-admin-api/src/pst/validate_uploaded_pst_file
---

# Signature

`pub(crate) fn validate_uploaded_pst_file_with_validator<D: Detector>( validator: &Validator<D>, path: &Path, file_name: &str, declared_mime: Option<&str>, ) -> anyhow::Result<()>`

# Calls

- [validate_path](../../../../../functions/crates/lpe-magika/src/validator/Validator/validate_path.md)
- [write_validation_record](../../../../../functions/crates/lpe-magika/src/record/write_validation_record.md)

# Called by

- [pst_upload_validation_accepts_valid_pst_like_file](../../../../../functions/crates/lpe-admin-api/src/app/pst_upload_validation_accepts_valid_pst_like_file.md)
- [pst_upload_validation_rejects_extension_and_type_mismatch](../../../../../functions/crates/lpe-admin-api/src/app/pst_upload_validation_rejects_extension_and_type_mismatch.md)
- [validate_uploaded_pst_file](../../../../../functions/crates/lpe-admin-api/src/pst/validate_uploaded_pst_file.md)