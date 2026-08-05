---
type: Rust Function
title: collection_id_from_contact_path
resource: crates/lpe-dav/src/paths.rs#L63-L65
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/paths/collection_id_from_path
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_propfind
  - functions/crates/lpe-dav/src/service/DavService/handle_report
---

# Signature

`pub(crate) fn collection_id_from_contact_path(path: &str) -> Option<String>`

# Calls

- [collection_id_from_path](../../../../../functions/crates/lpe-dav/src/paths/collection_id_from_path.md)

# Called by

- [handle_propfind](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_propfind.md)
- [handle_report](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_report.md)