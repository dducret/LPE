---
type: Rust Method
title: commit_mapi_event_update
resource: crates/lpe-exchange/src/tests/mod.rs#L8625-L8872
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/tests/test_mapi_pcl_includes_change_key
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-storage/src/attachments/calendar_attachment_file_reference
---

# Signature

`fn commit_mapi_event_update<'a>( &'a self, input: MapiEventCommitInput, ) -> StoreFuture<'a, MapiEventCommitOutcome>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [test_mapi_pcl_includes_change_key](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_pcl_includes_change_key.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [calendar_attachment_file_reference](../../../../../../../functions/crates/lpe-storage/src/attachments/calendar_attachment_file_reference.md)