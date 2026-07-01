use super::*;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(crate) async fn handle_mapi(
        &self,
        endpoint: MapiEndpoint,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        mapi::handle_mapi(&self.store, &self.validator, endpoint, headers, body).await
    }

    pub(crate) async fn handle_rpc_proxy(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        request_body: &[u8],
    ) -> Response {
        match authenticate_account(&self.store, None, headers, "mapi").await {
            Ok(principal) => {
                if let Some(connect) =
                    parse_rpc_proxy_out_data_connect_request(method, headers, request_body)
                {
                    if is_rpc_proxy_endpoint_ping(uri) {
                        rpc_proxy_mailstore_ping_response_for_connect(uri, connect)
                    } else {
                        rpc_proxy_rts_connect_response(connect.receive_window_size)
                    }
                } else if is_rpc_proxy_echo_request(method, headers) {
                    rpc_proxy_echo_response()
                } else {
                    rpc_proxy_accepted_response(&principal)
                }
            }
            Err(error) => rpc_proxy_auth_challenge_response(&error.to_string()),
        }
    }

    pub(crate) async fn handle_rpc_proxy_in_data_channel(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        body: Body,
    ) -> Response {
        match authenticate_account(&self.store, None, headers, "mapi").await {
            Ok(principal) => {
                spawn_rpc_proxy_in_data_drain(
                    self.store.clone(),
                    self.validator.clone(),
                    principal,
                    method,
                    uri,
                    headers,
                    body,
                );
                rpc_proxy_in_channel_response(uri)
            }
            Err(error) => rpc_proxy_auth_challenge_response(&error.to_string()),
        }
    }
}
