---
type: Rust Function
title: validation_sidecar_path
resource: crates/lpe-magika/src/record.rs#L38-L42
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-magika/src/record/write_validation_record
  - functions/crates/lpe-magika/src/record/read_validation_record
---

# Signature

`pub fn validation_sidecar_path(path: &Path) -> PathBuf`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [write_validation_record](../../../../../functions/crates/lpe-magika/src/record/write_validation_record.md)
- [read_validation_record](../../../../../functions/crates/lpe-magika/src/record/read_validation_record.md)