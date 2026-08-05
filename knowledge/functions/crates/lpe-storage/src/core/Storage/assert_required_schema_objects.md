---
type: Rust Method
title: assert_required_schema_objects
resource: crates/lpe-storage/src/core.rs#L70-L627
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/core/Storage/assert_schema_version
  - functions/crates/lpe-storage/src/core/startup_rejects_tagged_schema_without_required_mapi_shape
---

# Signature

`async fn assert_required_schema_objects(&self, schema_name: &str) -> Result<()>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [assert_schema_version](../../../../../../functions/crates/lpe-storage/src/core/Storage/assert_schema_version.md)
- [startup_rejects_tagged_schema_without_required_mapi_shape](../../../../../../functions/crates/lpe-storage/src/core/startup_rejects_tagged_schema_without_required_mapi_shape.md)