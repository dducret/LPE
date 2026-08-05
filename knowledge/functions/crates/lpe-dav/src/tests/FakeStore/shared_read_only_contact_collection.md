---
type: Rust Method
title: shared_read_only_contact_collection
resource: crates/lpe-dav/src/tests.rs#L132-L142
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/tests/FakeStore/shared_collection
  called_by:
  - functions/crates/lpe-dav/src/tests/propfind_lists_shared_contact_collection_with_read_only_privileges
  - functions/crates/lpe-dav/src/tests/report_filters_shared_contact_collection_by_shared_href
---

# Signature

`fn shared_read_only_contact_collection() -> CollaborationCollection`

# Calls

- [shared_collection](../../../../../../functions/crates/lpe-dav/src/tests/FakeStore/shared_collection.md)

# Called by

- [propfind_lists_shared_contact_collection_with_read_only_privileges](../../../../../../functions/crates/lpe-dav/src/tests/propfind_lists_shared_contact_collection_with_read_only_privileges.md)
- [report_filters_shared_contact_collection_by_shared_href](../../../../../../functions/crates/lpe-dav/src/tests/report_filters_shared_contact_collection_by_shared_href.md)