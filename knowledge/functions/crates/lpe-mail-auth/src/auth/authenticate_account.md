---
type: Rust Function
title: authenticate_account
resource: crates/lpe-mail-auth/src/auth.rs#L18-L49
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-mail-auth/src/auth/authenticate_bearer_access_token
  - functions/crates/lpe-mail-auth/src/oauth/basic_credentials
  - functions/crates/lpe-mail-auth/src/auth/authenticate_plain_credentials
  called_by:
  - functions/crates/lpe-activesync/src/app/options_response_for_store
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/authenticate
  - functions/crates/lpe-dav/src/service/DavService/handle
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy_in_data_channel
  - functions/crates/lpe-mail-auth/src/tests/bearer_session_preserves_tenant_id
  - functions/crates/lpe-mail-auth/src/tests/basic_auth_preserves_tenant_id
  - functions/crates/lpe-mail-auth/src/tests/hinted_user_does_not_override_login_tenant
  - functions/crates/lpe-mail-auth/src/tests/app_password_is_accepted_for_basic_auth
  - functions/crates/lpe-mail-auth/src/tests/oauth_access_token_is_accepted_for_bearer_auth
---

# Signature

`pub async fn authenticate_account<S: AccountAuthStore>( store: &S, hinted_user: Option<&str>, headers: &HeaderMap, surface: &str, ) -> Result<AccountPrincipal>`

# Calls

- [authenticate_bearer_access_token](../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_bearer_access_token.md)
- [basic_credentials](../../../../../functions/crates/lpe-mail-auth/src/oauth/basic_credentials.md)
- [authenticate_plain_credentials](../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_plain_credentials.md)

# Called by

- [options_response_for_store](../../../../../functions/crates/lpe-activesync/src/app/options_response_for_store.md)
- [authenticate](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/authenticate.md)
- [handle](../../../../../functions/crates/lpe-dav/src/service/DavService/handle.md)
- [handle_mapi](../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)
- [handle](../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)
- [handle_rpc_proxy](../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy.md)
- [handle_rpc_proxy_in_data_channel](../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy_in_data_channel.md)
- [bearer_session_preserves_tenant_id](../../../../../functions/crates/lpe-mail-auth/src/tests/bearer_session_preserves_tenant_id.md)
- [basic_auth_preserves_tenant_id](../../../../../functions/crates/lpe-mail-auth/src/tests/basic_auth_preserves_tenant_id.md)
- [hinted_user_does_not_override_login_tenant](../../../../../functions/crates/lpe-mail-auth/src/tests/hinted_user_does_not_override_login_tenant.md)
- [app_password_is_accepted_for_basic_auth](../../../../../functions/crates/lpe-mail-auth/src/tests/app_password_is_accepted_for_basic_auth.md)
- [oauth_access_token_is_accepted_for_bearer_auth](../../../../../functions/crates/lpe-mail-auth/src/tests/oauth_access_token_is_accepted_for_bearer_auth.md)