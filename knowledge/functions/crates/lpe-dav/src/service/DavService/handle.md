---
type: Rust Method
title: handle
resource: crates/lpe-dav/src/service.rs#L85-L106
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/paths/normalized_path
  - functions/crates/lpe-dav/src/responses/options_response
  - functions/crates/lpe-mail-auth/src/auth/authenticate_account
  - functions/crates/lpe-dav/src/service/DavService/handle_propfind
  - functions/crates/lpe-dav/src/service/DavService/handle_report
  - functions/crates/lpe-dav/src/service/DavService/handle_get
  - functions/crates/lpe-dav/src/service/DavService/handle_put
---

# Signature

`pub(crate) async fn handle( &self, method: &Method, uri: &Uri, headers: &HeaderMap, body: &[u8], ) -> Result<Response>`

# Calls

- [normalized_path](../../../../../../functions/crates/lpe-dav/src/paths/normalized_path.md)
- [options_response](../../../../../../functions/crates/lpe-dav/src/responses/options_response.md)
- [authenticate_account](../../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_account.md)
- [handle_propfind](../../../../../../functions/crates/lpe-dav/src/service/DavService/handle_propfind.md)
- [handle_report](../../../../../../functions/crates/lpe-dav/src/service/DavService/handle_report.md)
- [handle_get](../../../../../../functions/crates/lpe-dav/src/service/DavService/handle_get.md)
- [handle_put](../../../../../../functions/crates/lpe-dav/src/service/DavService/handle_put.md)