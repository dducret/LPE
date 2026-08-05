---
type: Rust Method
title: validate_path
resource: crates/lpe-magika/src/validator.rs#L47-L53
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/pst/validate_uploaded_pst_file_with_validator
  - functions/crates/lpe-attachments/src/extraction/extract_text_from_path
  - functions/crates/lpe-storage/src/pst/validate_pst_import_path
---

# Signature

`pub fn validate_path( &self, request: ValidationRequest, path: &Path, ) -> Result<ValidationOutcome>`

# Called by

- [validate_uploaded_pst_file_with_validator](../../../../../../functions/crates/lpe-admin-api/src/pst/validate_uploaded_pst_file_with_validator.md)
- [extract_text_from_path](../../../../../../functions/crates/lpe-attachments/src/extraction/extract_text_from_path.md)
- [validate_pst_import_path](../../../../../../functions/crates/lpe-storage/src/pst/validate_pst_import_path.md)