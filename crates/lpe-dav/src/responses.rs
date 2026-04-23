use anyhow::Error;
use axum::{http::StatusCode, response::Response};

pub(crate) fn response_entry(href: &str, propstat: String) -> String {
    format!("<d:response><d:href>{href}</d:href>{propstat}</d:response>")
}

pub(crate) fn multistatus_response(entries: Vec<String>) -> Response {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
<d:multistatus xmlns:d=\"DAV:\" xmlns:card=\"urn:ietf:params:xml:ns:carddav\" xmlns:cal=\"urn:ietf:params:xml:ns:caldav\">{}</d:multistatus>",
        entries.join("")
    );
    response_with_headers(
        207,
        "application/xml; charset=utf-8",
        body,
        &[("dav", "1, addressbook, calendar-access")],
    )
}

pub(crate) fn options_response() -> Response {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header("allow", "OPTIONS, PROPFIND, REPORT, GET, PUT, DELETE")
        .header("dav", "1, addressbook, calendar-access")
        .header("ms-author-via", "DAV")
        .body(axum::body::Body::empty())
        .unwrap()
}

pub(crate) fn redirect_response(location: &str) -> Response {
    Response::builder()
        .status(StatusCode::TEMPORARY_REDIRECT)
        .header("location", location)
        .body(axum::body::Body::empty())
        .unwrap()
}

pub(crate) fn text_response(content_type: &str, body: String, etag: Option<String>) -> Response {
    let mut headers = vec![("dav", "1, addressbook, calendar-access")];
    if let Some(ref value) = etag {
        headers.push(("etag", value.as_str()));
    }
    response_with_headers(200, content_type, body, &headers)
}

pub(crate) fn response_with_headers(
    status: u16,
    content_type: &str,
    body: String,
    headers: &[(&str, &str)],
) -> Response {
    let mut builder = Response::builder()
        .status(StatusCode::from_u16(status).unwrap())
        .header("content-type", content_type);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    builder.body(axum::body::Body::from(body)).unwrap()
}

pub(crate) fn status_only(status: u16) -> Response {
    Response::builder()
        .status(StatusCode::from_u16(status).unwrap())
        .body(axum::body::Body::empty())
        .unwrap()
}

pub(crate) fn status_with_etag(status: u16, etag: String) -> Response {
    Response::builder()
        .status(StatusCode::from_u16(status).unwrap())
        .header("etag", etag)
        .body(axum::body::Body::empty())
        .unwrap()
}

pub(crate) fn error_response(error: Error) -> Response {
    let message = error.to_string();
    if message.contains("missing account authentication") || message.contains("invalid credentials")
    {
        return Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header("www-authenticate", "Basic realm=\"LPE DAV\"")
            .body(axum::body::Body::from(message))
            .unwrap();
    }
    if message.contains("not found") {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(axum::body::Body::from(message))
            .unwrap();
    }
    if message.contains("method not allowed") {
        return Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(axum::body::Body::from(message))
            .unwrap();
    }
    if message.contains("precondition failed") {
        return Response::builder()
            .status(StatusCode::PRECONDITION_FAILED)
            .body(axum::body::Body::from(message))
            .unwrap();
    }
    if message.contains("forbidden") || message.contains("not granted") {
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(axum::body::Body::from(message))
            .unwrap();
    }
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(axum::body::Body::from(message))
        .unwrap()
}
