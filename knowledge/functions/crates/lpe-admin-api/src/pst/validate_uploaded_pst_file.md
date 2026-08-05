---
type: Rust Function
title: validate_uploaded_pst_file
resource: crates/lpe-admin-api/src/pst.rs#L21-L32
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/pst/validate_uploaded_pst_file_with_validator
  called_by:
  - functions/crates/lpe-admin-api/src/console/upload_pst_import
---

# Signature

`pub(crate) fn validate_uploaded_pst_file( path: &Path, file_name: &str, declared_mime: Option<&str>, ) -> anyhow::Result<()>`

# Calls

- [validate_uploaded_pst_file_with_validator](../../../../../functions/crates/lpe-admin-api/src/pst/validate_uploaded_pst_file_with_validator.md)

# Called by

- [upload_pst_import](../../../../../functions/crates/lpe-admin-api/src/console/upload_pst_import.md)