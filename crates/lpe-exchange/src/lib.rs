mod mapi;
mod mapi_mailstore;
mod mapi_store;
mod service;
mod store;

pub use crate::service::router;

#[cfg(test)]
mod tests;
