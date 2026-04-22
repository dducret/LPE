use anyhow::Result;
use lpe_storage::{
    AccountLogin, AuditEntryInput, AuthenticatedAccount, Storage, StoredAccountAppPassword,
};
use std::{future::Future, pin::Pin};

pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

pub trait AccountAuthStore: Clone + Send + Sync + 'static {
    fn fetch_account_session<'a>(
        &'a self,
        token: &'a str,
    ) -> StoreFuture<'a, Option<AuthenticatedAccount>>;
    fn fetch_account_login<'a>(&'a self, email: &'a str) -> StoreFuture<'a, Option<AccountLogin>>;
    fn fetch_active_account_app_passwords<'a>(
        &'a self,
        email: &'a str,
    ) -> StoreFuture<'a, Vec<StoredAccountAppPassword>>;
    fn touch_account_app_password<'a>(
        &'a self,
        email: &'a str,
        app_password_id: uuid::Uuid,
    ) -> StoreFuture<'a, ()>;
    fn append_audit_event<'a>(
        &'a self,
        tenant_id: &'a str,
        entry: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;
}

impl AccountAuthStore for Storage {
    fn fetch_account_session<'a>(
        &'a self,
        token: &'a str,
    ) -> StoreFuture<'a, Option<AuthenticatedAccount>> {
        Box::pin(async move { self.fetch_account_session(token).await })
    }

    fn fetch_account_login<'a>(&'a self, email: &'a str) -> StoreFuture<'a, Option<AccountLogin>> {
        Box::pin(async move { self.fetch_account_login(email).await })
    }

    fn fetch_active_account_app_passwords<'a>(
        &'a self,
        email: &'a str,
    ) -> StoreFuture<'a, Vec<StoredAccountAppPassword>> {
        Box::pin(async move { self.fetch_active_account_app_passwords(email).await })
    }

    fn touch_account_app_password<'a>(
        &'a self,
        email: &'a str,
        app_password_id: uuid::Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.touch_account_app_password(email, app_password_id)
                .await
        })
    }

    fn append_audit_event<'a>(
        &'a self,
        tenant_id: &'a str,
        entry: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.append_audit_event(tenant_id, entry).await })
    }
}
