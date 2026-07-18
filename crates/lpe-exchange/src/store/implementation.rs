use super::*;

include!("storage_impl/ews_admin.rs");
include!("storage_impl/ews_delegation.rs");
include!("storage_impl/mapi_replica_ids.rs");
include!("storage_impl/mapi_metadata.rs");
include!("storage_impl/mapi_permissions.rs");
include!("storage_impl/mapi_special_folder_aliases.rs");
include!("storage_impl/public_address_im.rs");
include!("storage_impl/collaboration.rs");
include!("storage_impl/mailbox_config.rs");
include!("storage_impl/messages.rs");

impl ExchangeStore for Storage {
    store_impl_ews_admin!();
    store_impl_ews_delegation!();
    store_impl_mapi_replica_ids!();
    store_impl_mapi_metadata!();
    store_impl_mapi_permissions!();
    store_impl_mapi_special_folder_aliases!();
    store_impl_public_address_im!();
    store_impl_collaboration!();
    store_impl_mailbox_config!();
    store_impl_messages!();
}

include!("storage_impl/address_helpers.rs");
include!("storage_impl/mapi_helpers.rs");
