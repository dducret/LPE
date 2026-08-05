---
type: Rust Function
title: collection_home_entry
resource: crates/lpe-dav/src/propfind.rs#L100-L109
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/responses/response_entry
  - functions/crates/lpe-dav/src/propfind/collection_props
---

# Signature

`pub(crate) fn collection_home_entry( path: &str, display_name: &str, resource_type: String, ) -> String`

# Calls

- [response_entry](../../../../../functions/crates/lpe-dav/src/responses/response_entry.md)
- [collection_props](../../../../../functions/crates/lpe-dav/src/propfind/collection_props.md)