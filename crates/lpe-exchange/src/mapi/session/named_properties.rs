use super::super::properties::*;
use super::*;

impl MapiSession {
    pub(in crate::mapi) fn property_id_for_name(
        &mut self,
        property: MapiNamedProperty,
        create: bool,
    ) -> Option<u16> {
        let property = normalize_named_property(property);
        if property.guid == PS_MAPI_GUID {
            if let MapiNamedPropertyKind::Lid(lid) = &property.kind {
                return u16::try_from(*lid).ok();
            }
        }
        if let Some(property_id) = canonical_calendar_named_property_id(&property) {
            self.named_properties.insert(property.clone(), property_id);
            self.named_property_ids.insert(property_id, property);
            return Some(property_id);
        }
        if let Some(property_id) = self.named_properties.get(&property).copied() {
            return Some(property_id);
        }
        if let Some(property_id) = well_known_named_property_id(&property) {
            self.named_properties.insert(property.clone(), property_id);
            self.named_property_ids.insert(property_id, property);
            return Some(property_id);
        }
        if !create || self.next_named_property_id > MAX_NAMED_PROPERTY_ID {
            return None;
        }

        self.next_named_property_id = self
            .next_named_property_id
            .max(DYNAMIC_NAMED_PROPERTY_ID_START);
        while self.next_named_property_id <= MAX_NAMED_PROPERTY_ID
            && (self
                .named_property_ids
                .contains_key(&self.next_named_property_id)
                || is_reserved_named_property_id(self.next_named_property_id))
        {
            self.next_named_property_id = self.next_named_property_id.saturating_add(1);
        }
        if self.next_named_property_id > MAX_NAMED_PROPERTY_ID {
            return None;
        }
        let property_id = self.next_named_property_id;
        self.next_named_property_id = self.next_named_property_id.saturating_add(1);
        self.named_properties.insert(property.clone(), property_id);
        self.named_property_ids.insert(property_id, property);
        Some(property_id)
    }

    pub(in crate::mapi) fn cache_named_property(
        &mut self,
        property_id: u16,
        property: MapiNamedProperty,
    ) -> Option<u16> {
        let property = normalize_named_property(property);
        let registered_property_id = property_id;
        let property_id = match canonical_calendar_named_property_id(&property) {
            Some(canonical_property_id)
                if registered_property_id != canonical_property_id
                    && canonical_calendar_named_property_for_id(registered_property_id)
                        .is_some() =>
            {
                return None;
            }
            Some(canonical_property_id) => canonical_property_id,
            None if canonical_calendar_named_property_for_id(property_id).is_some() => return None,
            None => property_id,
        };
        if registered_property_id != property_id {
            if let Some(previous_property) = self
                .named_property_ids
                .insert(registered_property_id, property.clone())
            {
                if previous_property != property
                    && self.named_properties.get(&previous_property)
                        == Some(&registered_property_id)
                {
                    self.named_properties.remove(&previous_property);
                }
            }
        }
        if let Some(previous_property_id) =
            self.named_properties.insert(property.clone(), property_id)
        {
            if previous_property_id != property_id
                && self.named_property_ids.get(&previous_property_id) == Some(&property)
            {
                self.named_property_ids.remove(&previous_property_id);
            }
        }
        if let Some(previous_property) = self
            .named_property_ids
            .insert(property_id, property.clone())
        {
            if previous_property != property
                && self.named_properties.get(&previous_property) == Some(&property_id)
            {
                self.named_properties.remove(&previous_property);
            }
        }
        let highest_property_id = property_id.max(registered_property_id);
        if highest_property_id >= self.next_named_property_id {
            self.next_named_property_id = highest_property_id.saturating_add(1);
        }
        Some(property_id)
    }

    pub(in crate::mapi) fn property_name_for_id(&self, property_id: u16) -> MapiNamedProperty {
        canonical_calendar_named_property_for_id(property_id)
            .or_else(|| self.named_property_ids.get(&property_id).cloned())
            .or_else(|| well_known_named_property_for_id(property_id))
            .unwrap_or(MapiNamedProperty {
                guid: PS_MAPI_GUID,
                kind: MapiNamedPropertyKind::Lid(u32::from(property_id)),
            })
    }

    pub(in crate::mapi) fn named_properties_for_query(
        &self,
        guid: Option<[u8; 16]>,
    ) -> Vec<(u16, MapiNamedProperty)> {
        let mut properties = self
            .named_property_ids
            .iter()
            .filter(|(property_id, property)| {
                self.named_properties.get(*property) == Some(*property_id)
                    && match guid {
                        Some(guid) => property.guid == guid,
                        None => true,
                    }
            })
            .map(|(property_id, property)| (*property_id, property.clone()))
            .collect::<Vec<_>>();
        properties.sort_by_key(|(property_id, _property)| *property_id);
        properties
    }
}

pub(in crate::mapi) fn normalize_named_property(
    mut property: MapiNamedProperty,
) -> MapiNamedProperty {
    if property.guid == PS_INTERNET_HEADERS_GUID {
        if let MapiNamedPropertyKind::Name(name) = property.kind {
            property.kind = MapiNamedPropertyKind::Name(name.to_ascii_lowercase());
        }
    }
    property
}
