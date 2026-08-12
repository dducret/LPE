use super::MapiObject;

#[derive(Clone, Copy, PartialEq, Eq)]
enum PropertyCopyObjectFamily {
    Folder,
    Message,
    Attachment,
}

fn property_copy_object_family(object: &MapiObject) -> Option<PropertyCopyObjectFamily> {
    // [MS-OXCPRPT] sections 2.2.10 and 2.2.11 limit these ROPs to Folder,
    // Message, and Attachment objects and require both objects to have the same type.
    match object {
        MapiObject::Folder { .. } => Some(PropertyCopyObjectFamily::Folder),
        MapiObject::Message { .. }
        | MapiObject::Contact { .. }
        | MapiObject::Event { .. }
        | MapiObject::Task { .. }
        | MapiObject::Note { .. }
        | MapiObject::JournalEntry { .. }
        | MapiObject::ConversationAction { .. }
        | MapiObject::NavigationShortcut { .. }
        | MapiObject::CommonViewNamedView { .. }
        | MapiObject::SearchFolderDefinitionMessage { .. }
        | MapiObject::AssociatedConfig { .. }
        | MapiObject::DelegateFreeBusyMessage { .. }
        | MapiObject::RecoverableItem { .. }
        | MapiObject::PublicFolderItem { .. }
        | MapiObject::PendingMessage { .. }
        | MapiObject::PendingAssociatedMessage { .. }
        | MapiObject::PendingContact { .. }
        | MapiObject::PendingEvent { .. }
        | MapiObject::PendingTask { .. }
        | MapiObject::PendingNote { .. }
        | MapiObject::PendingJournalEntry { .. }
        | MapiObject::PendingConversationAction { .. }
        | MapiObject::PendingNavigationShortcut { .. } => Some(PropertyCopyObjectFamily::Message),
        MapiObject::Attachment { .. }
        | MapiObject::PendingAttachment { .. }
        | MapiObject::SavedAttachment { .. } => Some(PropertyCopyObjectFamily::Attachment),
        _ => None,
    }
}

pub(super) fn property_copy_objects_are_compatible(
    source: &MapiObject,
    destination: &MapiObject,
) -> bool {
    property_copy_object_family(source)
        .zip(property_copy_object_family(destination))
        .is_some_and(|(source, destination)| source == destination)
}
