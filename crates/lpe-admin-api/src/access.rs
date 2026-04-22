use axum::http::{HeaderMap, StatusCode};
use lpe_storage::{AuthenticatedAccount, AuthenticatedAdmin, Storage};

use crate::http::{bearer_token, internal_error};

pub(crate) async fn require_admin(
    storage: &Storage,
    headers: &HeaderMap,
    right: &str,
) -> std::result::Result<AuthenticatedAdmin, (StatusCode, String)> {
    let token = bearer_token(headers)
        .ok_or((StatusCode::UNAUTHORIZED, "missing bearer token".to_string()))?;
    let admin = storage
        .fetch_admin_session(&token)
        .await
        .map_err(internal_error)?
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "invalid or expired session".to_string(),
        ))?;

    if admin_has_right(&admin, right) {
        Ok(admin)
    } else {
        Err((
            StatusCode::FORBIDDEN,
            "insufficient admin rights".to_string(),
        ))
    }
}

pub(crate) async fn require_account(
    storage: &Storage,
    headers: &HeaderMap,
) -> std::result::Result<AuthenticatedAccount, (StatusCode, String)> {
    let token = bearer_token(headers)
        .ok_or((StatusCode::UNAUTHORIZED, "missing bearer token".to_string()))?;
    storage
        .fetch_account_session(&token)
        .await
        .map_err(internal_error)?
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "invalid or expired session".to_string(),
        ))
}

fn admin_has_right(admin: &AuthenticatedAdmin, right: &str) -> bool {
    admin
        .permissions
        .iter()
        .any(|entry| entry == right || entry == "*")
}
