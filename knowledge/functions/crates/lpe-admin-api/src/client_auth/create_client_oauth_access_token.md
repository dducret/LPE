---
type: Rust Function
title: create_client_oauth_access_token
resource: crates/lpe-admin-api/src/client_auth.rs#L355-L399
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-mail-auth/src/oauth/normalize_scope
  - functions/crates/lpe-admin-api/src/security/client_oauth_access_token_seconds
  - functions/crates/lpe-mail-auth/src/oauth/issue_oauth_access_token
---

# Signature

`pub(crate) async fn create_client_oauth_access_token( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<CreateClientOauthAccessTokenRequest>, ) -> ApiResult<ClientOauthAccessTokenResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [normalize_scope](../../../../../functions/crates/lpe-mail-auth/src/oauth/normalize_scope.md)
- [client_oauth_access_token_seconds](../../../../../functions/crates/lpe-admin-api/src/security/client_oauth_access_token_seconds.md)
- [issue_oauth_access_token](../../../../../functions/crates/lpe-mail-auth/src/oauth/issue_oauth_access_token.md)