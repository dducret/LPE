use super::*;

const RECIPIENT_DELIVERY_PROPERTY_TAGS: [u32; 15] = [
    PID_TAG_MESSAGE_TO_ME,
    PID_TAG_MESSAGE_CC_ME,
    PID_TAG_MESSAGE_RECIPIENT_ME,
    PID_TAG_RECEIVED_BY_ADDRESS_TYPE_W,
    PID_TAG_RECEIVED_BY_EMAIL_ADDRESS_W,
    PID_TAG_RECEIVED_BY_ENTRY_ID_ALT,
    PID_TAG_RECEIVED_BY_NAME_W,
    PID_TAG_RECEIVED_BY_SEARCH_KEY,
    PID_TAG_RECEIVED_BY_SMTP_ADDRESS_W,
    PID_TAG_RECEIVED_REPRESENTING_ADDRESS_TYPE_W,
    PID_TAG_RECEIVED_REPRESENTING_EMAIL_ADDRESS_W,
    PID_TAG_RECEIVED_REPRESENTING_ENTRY_ID,
    PID_TAG_RECEIVED_REPRESENTING_NAME_W,
    PID_TAG_RECEIVED_REPRESENTING_SEARCH_KEY,
    PID_TAG_RECEIVED_REPRESENTING_SMTP_ADDRESS_W,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecipientDeliveryKind {
    To,
    Cc,
    Other,
}

pub(crate) fn email_recipient_delivery_property_tags(
    email: &JmapEmail,
    principal: &AccountPrincipal,
) -> Vec<u32> {
    recipient_delivery_kind(email, principal)
        .map(|_| RECIPIENT_DELIVERY_PROPERTY_TAGS.to_vec())
        .unwrap_or_default()
}

pub(crate) fn email_property_value_for_principal(
    email: &JmapEmail,
    principal: &AccountPrincipal,
    property_tag: u32,
) -> Option<MapiValue> {
    recipient_delivery_property_value(email, principal, property_tag)
        .or_else(|| email_property_value(email, property_tag))
}

pub(crate) fn email_property_value_with_durable_identity_for_principal(
    email: &JmapEmail,
    durable_identity: Option<&crate::store::MapiIdentityRecord>,
    principal: &AccountPrincipal,
    property_tag: u32,
) -> Option<MapiValue> {
    recipient_delivery_property_value(email, principal, property_tag).or_else(|| {
        email_property_value_with_durable_identity(email, durable_identity, property_tag)
    })
}

pub(crate) fn serialize_email_property_with_durable_identity_for_principal(
    email: &JmapEmail,
    durable_identity: Option<&crate::store::MapiIdentityRecord>,
    principal: &AccountPrincipal,
    property_tag: u32,
) -> Vec<u8> {
    let Some(property_value) = recipient_delivery_property_value(email, principal, property_tag)
    else {
        return serialize_message_row_with_durable_identity(
            email,
            durable_identity,
            &[property_tag],
        );
    };

    let mut value = Vec::new();
    write_mapi_value(&mut value, property_tag, &property_value);
    value
}

fn recipient_delivery_property_value(
    email: &JmapEmail,
    principal: &AccountPrincipal,
    property_tag: u32,
) -> Option<MapiValue> {
    let delivery_kind = recipient_delivery_kind(email, principal)?;
    let property_tag = canonical_property_storage_tag(property_tag);
    let entry = crate::mapi::nspi::principal_address_book_entry(principal);
    let legacy_dn = crate::mapi::nspi::nspi_entry_unprefixed_legacy_dn(&entry);
    let display_name = if principal.display_name.trim().is_empty() {
        principal.email.trim()
    } else {
        principal.display_name.trim()
    };

    // [MS-OXOMSG] sections 2.2.1.23-2.2.1.28 and 2.2.1.36-2.2.1.41:
    // a mailbox receiving on its own behalf uses the same resolved identity
    // for the ReceivedBy and ReceivedRepresenting property groups.
    match property_tag {
        PID_TAG_MESSAGE_TO_ME => Some(MapiValue::Bool(delivery_kind == RecipientDeliveryKind::To)),
        PID_TAG_MESSAGE_CC_ME => Some(MapiValue::Bool(delivery_kind == RecipientDeliveryKind::Cc)),
        // [MS-OXOMSG] section 2.2.1.19: RecipientMe is true for an
        // explicitly named To, Cc, or Bcc recipient.
        PID_TAG_MESSAGE_RECIPIENT_ME => Some(MapiValue::Bool(true)),
        PID_TAG_RECEIVED_BY_ADDRESS_TYPE_W | PID_TAG_RECEIVED_REPRESENTING_ADDRESS_TYPE_W => {
            Some(MapiValue::String("EX".to_string()))
        }
        PID_TAG_RECEIVED_BY_EMAIL_ADDRESS_W | PID_TAG_RECEIVED_REPRESENTING_EMAIL_ADDRESS_W => {
            Some(MapiValue::String(legacy_dn))
        }
        PID_TAG_RECEIVED_BY_ENTRY_ID_ALT | PID_TAG_RECEIVED_REPRESENTING_ENTRY_ID => Some(
            MapiValue::Binary(crate::mapi::nspi::nspi_entry_permanent_entry_id(&entry)),
        ),
        PID_TAG_RECEIVED_BY_NAME_W | PID_TAG_RECEIVED_REPRESENTING_NAME_W => {
            Some(MapiValue::String(display_name.to_string()))
        }
        PID_TAG_RECEIVED_BY_SEARCH_KEY | PID_TAG_RECEIVED_REPRESENTING_SEARCH_KEY => Some(
            MapiValue::Binary(crate::mapi::nspi::nspi_entry_search_key(&entry)),
        ),
        PID_TAG_RECEIVED_BY_SMTP_ADDRESS_W | PID_TAG_RECEIVED_REPRESENTING_SMTP_ADDRESS_W => {
            Some(MapiValue::String(principal.email.trim().to_string()))
        }
        _ => None,
    }
}

fn recipient_delivery_kind(
    email: &JmapEmail,
    principal: &AccountPrincipal,
) -> Option<RecipientDeliveryKind> {
    if matches!(email.mailbox_role.as_str(), "sent" | "drafts" | "outbox") {
        return None;
    }
    let principal_email = normalize_calendar_email(&principal.email);
    if principal_email.is_empty() {
        return None;
    }

    if let Some(response) = email.calendar_meeting_response.as_ref() {
        return response
            .organizer
            .as_ref()
            .filter(|organizer| recipient_matches(&organizer.email, &principal_email))
            .map(|_| RecipientDeliveryKind::To);
    }
    if let Some(request) = email.calendar_meeting_request.as_ref() {
        let mut matched = None;
        for attendee in request
            .attendees
            .iter()
            .filter(|attendee| recipient_matches(&attendee.email, &principal_email))
        {
            let kind = if matches!(attendee.role.as_str(), "CHAIR" | "REQ-PARTICIPANT") {
                RecipientDeliveryKind::To
            } else if attendee.role == "OPT-PARTICIPANT" {
                RecipientDeliveryKind::Cc
            } else if matches!(attendee.cutype.as_str(), "RESOURCE" | "ROOM")
                || attendee.role == "NON-PARTICIPANT"
            {
                RecipientDeliveryKind::Other
            } else {
                RecipientDeliveryKind::To
            };
            if kind == RecipientDeliveryKind::To {
                return Some(kind);
            }
            if kind == RecipientDeliveryKind::Cc || matched.is_none() {
                matched = Some(kind);
            }
        }
        return matched;
    }

    if email
        .to
        .iter()
        .any(|recipient| recipient_matches(&recipient.address, &principal_email))
    {
        return Some(RecipientDeliveryKind::To);
    }
    if email
        .cc
        .iter()
        .any(|recipient| recipient_matches(&recipient.address, &principal_email))
    {
        return Some(RecipientDeliveryKind::Cc);
    }
    email
        .bcc
        .iter()
        .any(|recipient| recipient_matches(&recipient.address, &principal_email))
        .then_some(RecipientDeliveryKind::Other)
}

fn recipient_matches(candidate: &str, principal_email: &str) -> bool {
    let candidate = normalize_calendar_email(candidate);
    !candidate.is_empty() && candidate.eq_ignore_ascii_case(principal_email)
}
