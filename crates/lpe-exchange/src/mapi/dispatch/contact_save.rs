use super::*;
use crate::store::{MapiContactCreateOutcome, MapiIdentityRecord};
use lpe_storage::{MapiContactImportConflict, MapiContactImportObjectDeleted};

#[allow(clippy::too_many_arguments)]
pub(super) async fn save_pending_contact<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    snapshot: &mut MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    handle: u32,
    folder_id: u64,
    properties: HashMap<u32, MapiValue>,
    imported_identity: Option<lpe_storage::MapiContactImportedIdentity>,
    fail_on_conflict: bool,
) {
    let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) else {
        responses.extend_from_slice(&rop_error_response(
            0x0C,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    if folder.kind != MapiCollaborationFolderKind::Contacts {
        responses.extend_from_slice(&rop_error_response(
            0x0C,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    }
    let input = contact_input_from_mapi(
        principal.account_id,
        None,
        &default_contact_for_mapping(principal.account_id, &folder.collection.id),
        &properties,
    );
    let create_input = MapiContactCreateInput {
        principal_account_id: principal.account_id,
        collection_id: folder.collection.id.clone(),
        mapi_folder_id: folder_id,
        contact: input,
        imported_identity,
        fail_on_conflict,
        custom_property_upserts: mapi_contact_custom_property_values_from_map(&properties),
    };
    match store.create_mapi_contact(create_input).await {
        Ok(MapiContactCreateOutcome::Created(created)) => {
            let changes_server_replica = created.import_disposition.changes_server_replica();
            let contact_id = created.mapi_object_id;
            let canonical_contact_id = created.contact.id;
            let identity = MapiIdentityRecord {
                object_kind: MapiIdentityObjectKind::Contact,
                canonical_id: canonical_contact_id,
                object_id: contact_id,
                change_number: created.version.change_number,
                source_key: crate::mapi::identity::source_key_for_object_id(contact_id),
                change_key: created.version.change_key.clone(),
                predecessor_change_list: created.version.predecessor_change_list.clone(),
                last_modification_time: created.version.last_modification_time,
            };
            snapshot.remember_created_contact(folder_id, created.contact, identity);
            session.handles.insert(
                handle,
                MapiObject::Contact {
                    folder_id,
                    contact_id,
                },
            );
            if changes_server_replica {
                session.record_notification(MapiNotificationEvent::content(
                    folder_id,
                    Some(contact_id),
                ));
            }
            // [MS-OXCFXICS] sections 3.1.5.3, 3.2.5.9.4.2, and 3.3.5.2.1:
            // acknowledge
            // the imported MID with the distinct server-assigned CN only
            // after Contact content and identity commit atomically.
            record_sync_upload_content_change(
                session,
                folder_id,
                contact_id,
                created.version.change_number,
                false,
                false,
            );
            append_save_changes_message_response(
                session,
                responses,
                handle_slots,
                request,
                handle,
                contact_id,
            );
        }
        Ok(MapiContactCreateOutcome::NotFound) => responses.extend_from_slice(&rop_error_response(
            0x0C,
            request.response_handle_index(),
            0x8004_010F,
        )),
        Ok(MapiContactCreateOutcome::AccessDenied) => responses.extend_from_slice(
            &rop_error_response(0x0C, request.response_handle_index(), 0x8007_0005),
        ),
        Err(error) => {
            let return_value = if error.is::<MapiContactImportObjectDeleted>() {
                // [MS-OXCFXICS] section 3.3.4.3.3.2.2.1 permits this warning
                // at SaveChangesMessage. [MS-OXCDATA] section 2.4 defines
                // ecObjectDeleted as 0x8004010A.
                0x8004_010A
            } else if error.is::<MapiContactImportConflict>() {
                0x8004_0109
            } else {
                0x8000_4005
            };
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                return_value,
            ));
        }
    }
}
