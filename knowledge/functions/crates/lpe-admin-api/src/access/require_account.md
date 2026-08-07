---
type: Rust Function
title: require_account
resource: crates/lpe-admin-api/src/access.rs#L32-L46
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/http/account_session_token
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/client_me
  - functions/crates/lpe-admin-api/src/client_auth/account_auth_factors
  - functions/crates/lpe-admin-api/src/client_auth/enroll_account_totp
  - functions/crates/lpe-admin-api/src/client_auth/verify_account_totp_factor
  - functions/crates/lpe-admin-api/src/client_auth/revoke_account_factor
  - functions/crates/lpe-admin-api/src/client_auth/list_account_app_passwords
  - functions/crates/lpe-admin-api/src/client_auth/create_account_app_password
  - functions/crates/lpe-admin-api/src/client_auth/revoke_account_app_password
  - functions/crates/lpe-admin-api/src/client_auth/create_client_oauth_access_token
  - functions/crates/lpe-admin-api/src/delegation/list_collaboration_overview
  - functions/crates/lpe-admin-api/src/delegation/upsert_collaboration_grant
  - functions/crates/lpe-admin-api/src/delegation/upsert_calendar_collection_grant
  - functions/crates/lpe-admin-api/src/delegation/delete_collaboration_grant
  - functions/crates/lpe-admin-api/src/delegation/delete_calendar_collection_grant
  - functions/crates/lpe-admin-api/src/delegation/upsert_task_list_grant
  - functions/crates/lpe-admin-api/src/delegation/delete_task_list_grant
  - functions/crates/lpe-admin-api/src/delegation/get_mailbox_delegation
  - functions/crates/lpe-admin-api/src/delegation/get_free_busy
  - functions/crates/lpe-admin-api/src/delegation/upsert_mailbox_delegation_grant
  - functions/crates/lpe-admin-api/src/delegation/delete_mailbox_delegation_grant
  - functions/crates/lpe-admin-api/src/delegation/upsert_sender_delegation_grant
  - functions/crates/lpe-admin-api/src/delegation/delete_sender_delegation_grant
  - functions/crates/lpe-admin-api/src/sieve/list_mailbox_rules
  - functions/crates/lpe-admin-api/src/sieve/get_sieve_overview
  - functions/crates/lpe-admin-api/src/sieve/get_sieve_script
  - functions/crates/lpe-admin-api/src/sieve/put_sieve_script
  - functions/crates/lpe-admin-api/src/sieve/rename_sieve_script
  - functions/crates/lpe-admin-api/src/sieve/set_active_sieve_script
  - functions/crates/lpe-admin-api/src/sieve/delete_sieve_script
  - functions/crates/lpe-admin-api/src/workspace/client_workspace
  - functions/crates/lpe-admin-api/src/workspace/save_draft_message
  - functions/crates/lpe-admin-api/src/workspace/delete_draft_message
  - functions/crates/lpe-admin-api/src/workspace/upsert_client_contact
  - functions/crates/lpe-admin-api/src/workspace/list_contact_books
  - functions/crates/lpe-admin-api/src/workspace/list_client_contacts
  - functions/crates/lpe-admin-api/src/workspace/get_client_contact
  - functions/crates/lpe-admin-api/src/workspace/patch_client_contact
  - functions/crates/lpe-admin-api/src/workspace/delete_client_contact
  - functions/crates/lpe-admin-api/src/workspace/query_recipient_suggestions
  - functions/crates/lpe-admin-api/src/workspace/dismiss_recipient_suggestion
  - functions/crates/lpe-admin-api/src/workspace/upsert_client_event
  - functions/crates/lpe-admin-api/src/workspace/delete_client_event
  - functions/crates/lpe-admin-api/src/workspace/list_client_tasks
  - functions/crates/lpe-admin-api/src/workspace/list_client_task_lists
  - functions/crates/lpe-admin-api/src/workspace/get_client_task
  - functions/crates/lpe-admin-api/src/workspace/upsert_client_task
  - functions/crates/lpe-admin-api/src/workspace/delete_client_task
  - functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_trees
  - functions/crates/lpe-admin-api/src/workspace/public_folders/create_public_folder_tree
  - functions/crates/lpe-admin-api/src/workspace/public_folders/get_public_folder
  - functions/crates/lpe-admin-api/src/workspace/public_folders/update_public_folder
  - functions/crates/lpe-admin-api/src/workspace/public_folders/delete_public_folder
  - functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_children
  - functions/crates/lpe-admin-api/src/workspace/public_folders/create_public_folder_child
  - functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_items
  - functions/crates/lpe-admin-api/src/workspace/public_folders/post_public_folder_item
  - functions/crates/lpe-admin-api/src/workspace/public_folders/patch_public_folder_item
  - functions/crates/lpe-admin-api/src/workspace/public_folders/delete_public_folder_item
  - functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_permissions
  - functions/crates/lpe-admin-api/src/workspace/public_folders/put_public_folder_permission
  - functions/crates/lpe-admin-api/src/workspace/public_folders/delete_public_folder_permission
  - functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_replicas
  - functions/crates/lpe-admin-api/src/workspace/public_folders/put_public_folder_replica
  - functions/crates/lpe-admin-api/src/workspace/public_folders/delete_public_folder_replica
  - functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_per_user_state
  - functions/crates/lpe-admin-api/src/workspace/public_folders/patch_public_folder_per_user_state
---

# Signature

`pub(crate) async fn require_account( storage: &Storage, headers: &HeaderMap, ) -> std::result::Result<AuthenticatedAccount, (StatusCode, String)>`

# Calls

- [account_session_token](../../../../../functions/crates/lpe-admin-api/src/http/account_session_token.md)

# Called by

- [client_me](../../../../../functions/crates/lpe-admin-api/src/client_auth/client_me.md)
- [account_auth_factors](../../../../../functions/crates/lpe-admin-api/src/client_auth/account_auth_factors.md)
- [enroll_account_totp](../../../../../functions/crates/lpe-admin-api/src/client_auth/enroll_account_totp.md)
- [verify_account_totp_factor](../../../../../functions/crates/lpe-admin-api/src/client_auth/verify_account_totp_factor.md)
- [revoke_account_factor](../../../../../functions/crates/lpe-admin-api/src/client_auth/revoke_account_factor.md)
- [list_account_app_passwords](../../../../../functions/crates/lpe-admin-api/src/client_auth/list_account_app_passwords.md)
- [create_account_app_password](../../../../../functions/crates/lpe-admin-api/src/client_auth/create_account_app_password.md)
- [revoke_account_app_password](../../../../../functions/crates/lpe-admin-api/src/client_auth/revoke_account_app_password.md)
- [create_client_oauth_access_token](../../../../../functions/crates/lpe-admin-api/src/client_auth/create_client_oauth_access_token.md)
- [list_collaboration_overview](../../../../../functions/crates/lpe-admin-api/src/delegation/list_collaboration_overview.md)
- [upsert_collaboration_grant](../../../../../functions/crates/lpe-admin-api/src/delegation/upsert_collaboration_grant.md)
- [upsert_calendar_collection_grant](../../../../../functions/crates/lpe-admin-api/src/delegation/upsert_calendar_collection_grant.md)
- [delete_collaboration_grant](../../../../../functions/crates/lpe-admin-api/src/delegation/delete_collaboration_grant.md)
- [delete_calendar_collection_grant](../../../../../functions/crates/lpe-admin-api/src/delegation/delete_calendar_collection_grant.md)
- [upsert_task_list_grant](../../../../../functions/crates/lpe-admin-api/src/delegation/upsert_task_list_grant.md)
- [delete_task_list_grant](../../../../../functions/crates/lpe-admin-api/src/delegation/delete_task_list_grant.md)
- [get_mailbox_delegation](../../../../../functions/crates/lpe-admin-api/src/delegation/get_mailbox_delegation.md)
- [get_free_busy](../../../../../functions/crates/lpe-admin-api/src/delegation/get_free_busy.md)
- [upsert_mailbox_delegation_grant](../../../../../functions/crates/lpe-admin-api/src/delegation/upsert_mailbox_delegation_grant.md)
- [delete_mailbox_delegation_grant](../../../../../functions/crates/lpe-admin-api/src/delegation/delete_mailbox_delegation_grant.md)
- [upsert_sender_delegation_grant](../../../../../functions/crates/lpe-admin-api/src/delegation/upsert_sender_delegation_grant.md)
- [delete_sender_delegation_grant](../../../../../functions/crates/lpe-admin-api/src/delegation/delete_sender_delegation_grant.md)
- [list_mailbox_rules](../../../../../functions/crates/lpe-admin-api/src/sieve/list_mailbox_rules.md)
- [get_sieve_overview](../../../../../functions/crates/lpe-admin-api/src/sieve/get_sieve_overview.md)
- [get_sieve_script](../../../../../functions/crates/lpe-admin-api/src/sieve/get_sieve_script.md)
- [put_sieve_script](../../../../../functions/crates/lpe-admin-api/src/sieve/put_sieve_script.md)
- [rename_sieve_script](../../../../../functions/crates/lpe-admin-api/src/sieve/rename_sieve_script.md)
- [set_active_sieve_script](../../../../../functions/crates/lpe-admin-api/src/sieve/set_active_sieve_script.md)
- [delete_sieve_script](../../../../../functions/crates/lpe-admin-api/src/sieve/delete_sieve_script.md)
- [client_workspace](../../../../../functions/crates/lpe-admin-api/src/workspace/client_workspace.md)
- [save_draft_message](../../../../../functions/crates/lpe-admin-api/src/workspace/save_draft_message.md)
- [delete_draft_message](../../../../../functions/crates/lpe-admin-api/src/workspace/delete_draft_message.md)
- [upsert_client_contact](../../../../../functions/crates/lpe-admin-api/src/workspace/upsert_client_contact.md)
- [list_contact_books](../../../../../functions/crates/lpe-admin-api/src/workspace/list_contact_books.md)
- [list_client_contacts](../../../../../functions/crates/lpe-admin-api/src/workspace/list_client_contacts.md)
- [get_client_contact](../../../../../functions/crates/lpe-admin-api/src/workspace/get_client_contact.md)
- [patch_client_contact](../../../../../functions/crates/lpe-admin-api/src/workspace/patch_client_contact.md)
- [delete_client_contact](../../../../../functions/crates/lpe-admin-api/src/workspace/delete_client_contact.md)
- [query_recipient_suggestions](../../../../../functions/crates/lpe-admin-api/src/workspace/query_recipient_suggestions.md)
- [dismiss_recipient_suggestion](../../../../../functions/crates/lpe-admin-api/src/workspace/dismiss_recipient_suggestion.md)
- [upsert_client_event](../../../../../functions/crates/lpe-admin-api/src/workspace/upsert_client_event.md)
- [delete_client_event](../../../../../functions/crates/lpe-admin-api/src/workspace/delete_client_event.md)
- [list_client_tasks](../../../../../functions/crates/lpe-admin-api/src/workspace/list_client_tasks.md)
- [list_client_task_lists](../../../../../functions/crates/lpe-admin-api/src/workspace/list_client_task_lists.md)
- [get_client_task](../../../../../functions/crates/lpe-admin-api/src/workspace/get_client_task.md)
- [upsert_client_task](../../../../../functions/crates/lpe-admin-api/src/workspace/upsert_client_task.md)
- [delete_client_task](../../../../../functions/crates/lpe-admin-api/src/workspace/delete_client_task.md)
- [list_public_folder_trees](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_trees.md)
- [create_public_folder_tree](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/create_public_folder_tree.md)
- [get_public_folder](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/get_public_folder.md)
- [update_public_folder](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/update_public_folder.md)
- [delete_public_folder](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/delete_public_folder.md)
- [list_public_folder_children](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_children.md)
- [create_public_folder_child](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/create_public_folder_child.md)
- [list_public_folder_items](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_items.md)
- [post_public_folder_item](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/post_public_folder_item.md)
- [patch_public_folder_item](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/patch_public_folder_item.md)
- [delete_public_folder_item](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/delete_public_folder_item.md)
- [list_public_folder_permissions](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_permissions.md)
- [put_public_folder_permission](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/put_public_folder_permission.md)
- [delete_public_folder_permission](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/delete_public_folder_permission.md)
- [list_public_folder_replicas](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_replicas.md)
- [put_public_folder_replica](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/put_public_folder_replica.md)
- [delete_public_folder_replica](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/delete_public_folder_replica.md)
- [list_public_folder_per_user_state](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_per_user_state.md)
- [patch_public_folder_per_user_state](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/patch_public_folder_per_user_state.md)