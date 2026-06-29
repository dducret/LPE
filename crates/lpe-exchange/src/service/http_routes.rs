pub(super) const EWS_PATH: &str = "/EWS/Exchange.asmx";
pub(super) const EWS_LOWER_PATH: &str = "/ews/exchange.asmx";
pub(super) const MAPI_EMSMDB_PATH: &str = "/mapi/emsmdb";
pub(super) const MAPI_EMSMDB_TRAILING_PATH: &str = "/mapi/emsmdb/";
pub(super) const MAPI_NSPI_PATH: &str = "/mapi/nspi";
pub(super) const MAPI_NSPI_TRAILING_PATH: &str = "/mapi/nspi/";
pub(super) const RPC_PROXY_PATH: &str = "/rpc/rpcproxy.dll";
pub(super) const RPC_PROXY_OUTLOOK_CANONICAL_PATH: &str = "/RPC/RpcProxy.dll";

pub(super) fn rpc_proxy_paths() -> [&'static str; 2] {
    [RPC_PROXY_PATH, RPC_PROXY_OUTLOOK_CANONICAL_PATH]
}

#[cfg(test)]
mod tests {
    use super::{rpc_proxy_paths, RPC_PROXY_OUTLOOK_CANONICAL_PATH, RPC_PROXY_PATH};

    #[test]
    fn rpc_proxy_routes_include_outlook_canonical_case() {
        let paths = rpc_proxy_paths();

        assert!(paths.contains(&RPC_PROXY_PATH));
        assert!(paths.contains(&RPC_PROXY_OUTLOOK_CANONICAL_PATH));
        assert_eq!(RPC_PROXY_OUTLOOK_CANONICAL_PATH, "/RPC/RpcProxy.dll");
    }
}
