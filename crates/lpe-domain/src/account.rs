use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccountId(pub Uuid);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Account {
    pub id: AccountId,
    pub primary_email: String,
    pub display_name: String,
}

impl Account {
    pub fn new(primary_email: impl Into<String>, display_name: impl Into<String>) -> Self {
        Self {
            id: AccountId(Uuid::new_v4()),
            primary_email: primary_email.into(),
            display_name: display_name.into(),
        }
    }
}
