mod ews_types;
mod mapi;
mod mapi_mailstore;
mod mapi_store;
#[cfg(test)]
mod microsoft_protocol_audit;
mod ntlm;
mod service;
mod store;

pub use crate::service::router;

#[cfg(test)]
mod tests;
