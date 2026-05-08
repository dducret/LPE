mod auth;
mod oauth;
mod store;

pub use crate::auth::{
    authenticate_account, authenticate_bearer_access_token, authenticate_plain_credentials,
    normalize_login_name, verify_password,
};
pub use crate::oauth::{
    basic_credentials, bearer_token, issue_oauth_access_token, normalize_scope,
    oauth_signing_secret, unix_time, AccountPrincipal, DEFAULT_OAUTH_ACCESS_SCOPE,
    DEFAULT_OAUTH_ACCESS_TOKEN_SECONDS,
};
pub use crate::store::{AccountAuthStore, StoreFuture};

#[cfg(test)]
mod tests;
