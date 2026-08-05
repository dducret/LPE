---
type: Rust Function
title: options_response_for_store
resource: crates/lpe-activesync/src/app.rs#L39-L48
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-mail-auth/src/auth/authenticate_account
  - functions/crates/lpe-activesync/src/response/empty_response
  - functions/crates/lpe-activesync/src/response/auth_challenge_response
  called_by:
  - functions/crates/lpe-activesync/src/app/options_handler
  - functions/crates/lpe-activesync/src/tests/options_challenges_anonymous_requests
  - functions/crates/lpe-activesync/src/tests/options_returns_capabilities_after_authentication
---

# Signature

`pub(crate) async fn options_response_for_store<S: ActiveSyncStore>( storage: &S, query: &ActiveSyncQuery, headers: &HeaderMap, ) -> Response`

# Calls

- [authenticate_account](../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_account.md)
- [empty_response](../../../../../functions/crates/lpe-activesync/src/response/empty_response.md)
- [auth_challenge_response](../../../../../functions/crates/lpe-activesync/src/response/auth_challenge_response.md)

# Called by

- [options_handler](../../../../../functions/crates/lpe-activesync/src/app/options_handler.md)
- [options_challenges_anonymous_requests](../../../../../functions/crates/lpe-activesync/src/tests/options_challenges_anonymous_requests.md)
- [options_returns_capabilities_after_authentication](../../../../../functions/crates/lpe-activesync/src/tests/options_returns_capabilities_after_authentication.md)