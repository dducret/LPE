---
type: Rust Function
title: associated_config_debug_fields
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config.rs#L3-L34
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response
---

# Signature

`pub(in crate::mapi::dispatch) fn associated_config_debug_fields( session: &MapiSession, snapshot: &MapiMailStoreSnapshot, handle: u32, ) -> (String, String, String)`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [associated_config_message_for_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id.md)

# Called by

- [append_open_stream_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response.md)