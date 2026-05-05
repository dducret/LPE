use anyhow::Error;
use axum::{http::StatusCode, Json};
use serde_json::{json, Value};

pub(crate) const JMAP_PROBLEM_UNKNOWN_CAPABILITY: &str =
    "urn:ietf:params:jmap:error:unknownCapability";
pub(crate) const JMAP_PROBLEM_NOT_REQUEST: &str = "urn:ietf:params:jmap:error:notRequest";
pub(crate) const JMAP_PROBLEM_LIMIT: &str = "urn:ietf:params:jmap:error:limit";

pub(crate) fn http_error(error: Error) -> (StatusCode, Json<Value>) {
    let message = error.to_string();
    if message.contains("bearer token") || message.contains("expired account session") {
        jmap_problem("about:blank", StatusCode::UNAUTHORIZED, message, None)
    } else if message.contains("Magika command")
        || message.contains("spawn Magika")
        || message.contains("Magika stdin")
    {
        jmap_problem(
            "about:blank",
            StatusCode::INTERNAL_SERVER_ERROR,
            message,
            None,
        )
    } else {
        jmap_problem(
            JMAP_PROBLEM_NOT_REQUEST,
            StatusCode::BAD_REQUEST,
            message,
            None,
        )
    }
}

pub(crate) fn jmap_problem(
    problem_type: &str,
    status: StatusCode,
    detail: impl Into<String>,
    limit: Option<&str>,
) -> (StatusCode, Json<Value>) {
    let mut object = serde_json::Map::new();
    object.insert("type".to_string(), Value::String(problem_type.to_string()));
    object.insert(
        "status".to_string(),
        Value::Number(serde_json::Number::from(status.as_u16())),
    );
    object.insert("detail".to_string(), Value::String(detail.into()));
    if let Some(limit) = limit {
        object.insert("limit".to_string(), Value::String(limit.to_string()));
    }
    (status, Json(Value::Object(object)))
}

pub(crate) fn method_error(kind: &str, description: &str) -> Value {
    json!({
        "type": kind,
        "description": description,
    })
}

pub(crate) fn set_error(description: &str) -> Value {
    method_error("invalidProperties", description)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jmap_problem_details_include_status_and_limit() {
        let (status, Json(body)) = jmap_problem(
            JMAP_PROBLEM_LIMIT,
            StatusCode::PAYLOAD_TOO_LARGE,
            "too many calls",
            Some("maxCallsInRequest"),
        );

        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(body["type"], JMAP_PROBLEM_LIMIT);
        assert_eq!(body["status"], 413);
        assert_eq!(body["detail"], "too many calls");
        assert_eq!(body["limit"], "maxCallsInRequest");
    }
}
