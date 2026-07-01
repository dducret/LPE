use axum::{
    routing::{any, on, MethodFilter},
    Router,
};
use lpe_storage::Storage;

use super::{
    mapi_emsmdb_post_handler, mapi_nspi_post_handler, mapi_options_handler, options_handler,
    post_handler, rpc_proxy_handler,
};

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

pub(super) fn exchange_router() -> Router<Storage> {
    let router = Router::new()
        .route(
            EWS_PATH,
            on(MethodFilter::OPTIONS, options_handler).post(post_handler),
        )
        .route(
            EWS_LOWER_PATH,
            on(MethodFilter::OPTIONS, options_handler).post(post_handler),
        )
        .route(
            MAPI_EMSMDB_PATH,
            on(MethodFilter::OPTIONS, mapi_options_handler).post(mapi_emsmdb_post_handler),
        )
        .route(
            MAPI_EMSMDB_TRAILING_PATH,
            on(MethodFilter::OPTIONS, mapi_options_handler).post(mapi_emsmdb_post_handler),
        )
        .route(
            MAPI_NSPI_PATH,
            on(MethodFilter::OPTIONS, mapi_options_handler).post(mapi_nspi_post_handler),
        )
        .route(
            MAPI_NSPI_TRAILING_PATH,
            on(MethodFilter::OPTIONS, mapi_options_handler).post(mapi_nspi_post_handler),
        );
    rpc_proxy_paths().into_iter().fold(router, |router, path| {
        router.route(path, any(rpc_proxy_handler))
    })
}

#[cfg(test)]
mod tests {
    use super::{
        exchange_router, rpc_proxy_paths, RPC_PROXY_OUTLOOK_CANONICAL_PATH, RPC_PROXY_PATH,
    };

    #[test]
    fn rpc_proxy_routes_include_outlook_canonical_case() {
        let paths = rpc_proxy_paths();

        assert!(paths.contains(&RPC_PROXY_PATH));
        assert!(paths.contains(&RPC_PROXY_OUTLOOK_CANONICAL_PATH));
        assert_eq!(RPC_PROXY_OUTLOOK_CANONICAL_PATH, "/RPC/RpcProxy.dll");
    }

    #[test]
    fn exchange_router_builds_with_all_route_families() {
        let _ = exchange_router();
    }
}
