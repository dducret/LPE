use axum::{
    http::{
        header::{CONTENT_TYPE, WWW_AUTHENTICATE},
        HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
};
use lpe_mail_auth::AccountPrincipal;

use super::RPC_PROXY_COMPAT_STATUS;

pub(super) fn rpc_proxy_accepted_response(principal: &AccountPrincipal) -> Response {
    let mut response = (
        StatusCode::OK,
        format!(
            "LPE RPC proxy compatibility authentication accepted for {}. Use MAPI over HTTP for mailbox access.\n",
            principal.email
        ),
    )
        .into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        RPC_PROXY_COMPAT_STATUS,
        HeaderValue::from_static("auth-accepted"),
    );
    response
}

pub(super) fn rpc_proxy_auth_challenge_response(message: &str) -> Response {
    let mut response = (
        StatusCode::UNAUTHORIZED,
        format!("LPE RPC proxy authentication required: {message}\n"),
    )
        .into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"LPE RPC\""),
    );
    response
}
