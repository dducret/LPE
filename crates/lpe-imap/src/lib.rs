mod acl;
mod append;
mod auth;
mod idle;
mod mailboxes;
mod messages;
mod parse;
mod render;
mod search;
mod service;
mod store;
mod store_args;
mod uid;

pub use service::{serve, ImapServer};

pub(crate) use service::{MessageRefKind, SelectedMailbox, Session, UID_VALIDITY};

#[cfg(test)]
mod tests;
