use anyhow::Result;
use lpe_mail_auth::AccountAuthStore;
use lpe_storage::{AuditEntryInput, SieveScriptDocument, SieveScriptSummary, Storage};
use std::{future::Future, pin::Pin};
use uuid::Uuid;

pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

pub trait ManageSieveStore: AccountAuthStore {
    fn list_sieve_scripts<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<SieveScriptSummary>>;
    fn get_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
    ) -> StoreFuture<'a, Option<SieveScriptDocument>>;
    fn put_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        content: &'a str,
        activate: bool,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SieveScriptDocument>;
    fn delete_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;
    fn rename_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        old_name: &'a str,
        new_name: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SieveScriptSummary>;
    fn set_active_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: Option<&'a str>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<String>>;
}

impl ManageSieveStore for Storage {
    fn list_sieve_scripts<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<SieveScriptSummary>> {
        Box::pin(async move { self.list_sieve_scripts(account_id).await })
    }

    fn get_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
    ) -> StoreFuture<'a, Option<SieveScriptDocument>> {
        Box::pin(async move { self.get_sieve_script(account_id, name).await })
    }

    fn put_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        content: &'a str,
        activate: bool,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SieveScriptDocument> {
        Box::pin(async move {
            self.put_sieve_script(account_id, name, content, activate, audit)
                .await
        })
    }

    fn delete_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_sieve_script(account_id, name, audit).await })
    }

    fn rename_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        old_name: &'a str,
        new_name: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SieveScriptSummary> {
        Box::pin(async move {
            self.rename_sieve_script(account_id, old_name, new_name, audit)
                .await
        })
    }

    fn set_active_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: Option<&'a str>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<String>> {
        Box::pin(async move { self.set_active_sieve_script(account_id, name, audit).await })
    }
}
