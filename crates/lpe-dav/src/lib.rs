mod parse;
mod paths;
mod preconditions;
mod propfind;
mod report;
mod responses;
mod serialize;
mod service;
mod store;

pub use crate::service::router;

#[cfg(test)]
mod tests;
