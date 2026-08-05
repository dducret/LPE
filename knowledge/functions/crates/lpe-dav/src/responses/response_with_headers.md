---
type: Rust Function
title: response_with_headers
resource: crates/lpe-dav/src/responses.rs#L48-L61
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/HostLogError/status
  called_by:
  - functions/crates/lpe-dav/src/responses/multistatus_response
  - functions/crates/lpe-dav/src/responses/text_response
---

# Signature

`pub(crate) fn response_with_headers( status: u16, content_type: &str, body: String, headers: &[(&str, &str)], ) -> Response`

# Calls

- [status](../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)

# Called by

- [multistatus_response](../../../../../functions/crates/lpe-dav/src/responses/multistatus_response.md)
- [text_response](../../../../../functions/crates/lpe-dav/src/responses/text_response.md)