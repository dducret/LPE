---
type: Rust Method
title: record_outlook_stream_batch_observed
resource: crates/lpe-exchange/src/mapi/session.rs#L903-L918
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/record_execute_stream_batch_observation
---

# Signature

`pub(in crate::mapi) fn record_outlook_stream_batch_observed(&mut self, summary: String)`

# Calls

- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [record_execute_stream_batch_observation](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/record_execute_stream_batch_observation.md)