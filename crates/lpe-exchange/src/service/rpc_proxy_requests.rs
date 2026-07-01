use super::*;

pub(super) fn is_rpc_proxy_echo_request(method: &Method, headers: &HeaderMap) -> bool {
    let method = method.as_str();
    if method != "RPC_IN_DATA" && method != "RPC_OUT_DATA" {
        return false;
    }

    is_rpc_proxy_msrpc_request(headers)
}

pub(crate) fn is_rpc_proxy_in_data_channel_request(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> bool {
    method.as_str() == "RPC_IN_DATA"
        && is_rpc_proxy_endpoint_ping(uri)
        && is_rpc_proxy_msrpc_request(headers)
        && !is_rpc_proxy_zero_length_request(headers)
}

fn is_rpc_proxy_zero_length_request(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .is_some_and(|length| length == 0)
}

pub(super) fn is_rpc_proxy_endpoint_ping(uri: &Uri) -> bool {
    uri.query().is_some_and(is_rpc_proxy_endpoint_query)
}

pub(super) fn is_rpc_proxy_endpoint_query(query: &str) -> bool {
    query.contains(":6001") || query.contains(":6002") || query.contains(":6004")
}

pub(super) fn is_rpc_proxy_msrpc_request(headers: &HeaderMap) -> bool {
    let user_agent = mapi::safe_header(headers, "user-agent")
        .unwrap_or_default()
        .to_ascii_lowercase();
    let accept = mapi::safe_header(headers, "accept")
        .unwrap_or_default()
        .to_ascii_lowercase();
    user_agent == "msrpc" || accept.contains("application/rpc")
}
