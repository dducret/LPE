---
type: Rust Function
title: rop_restrict_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L385-L390
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/restrict_response
---

# Signature

`pub(in crate::mapi) fn rop_restrict_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [restrict_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/restrict_response.md)