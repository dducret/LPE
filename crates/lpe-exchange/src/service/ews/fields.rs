use super::super::*;

pub(in crate::service) trait EmptyStringFallback {
    fn if_empty(self, fallback: String) -> String;
}

impl EmptyStringFallback for String {
    fn if_empty(self, fallback: String) -> String {
        if self.trim().is_empty() {
            fallback
        } else {
            self
        }
    }
}

pub(in crate::service) fn deleted_or_updated_text(
    request: &str,
    container: &str,
    field_uri: &str,
    local_name: &str,
    existing: &str,
) -> String {
    if field_deleted(request, field_uri) {
        String::new()
    } else {
        element_text(container, local_name).unwrap_or_else(|| existing.to_string())
    }
}

pub(in crate::service) fn field_deleted(request: &str, field_uri: &str) -> bool {
    element_contents(request, "DeleteItemField")
        .into_iter()
        .any(|delete| field_block_matches(delete, field_uri))
}

fn field_block_matches(block: &str, field_uri: &str) -> bool {
    if attribute_values_for_tag(block, "FieldURI", "FieldURI")
        .into_iter()
        .any(|value| value == field_uri)
    {
        return true;
    }

    let Some((base_field_uri, field_index)) = field_uri.rsplit_once(':') else {
        return false;
    };
    let indexed_fields = attribute_values_for_tag(block, "IndexedFieldURI", "FieldURI");
    let field_indexes = attribute_values_for_tag(block, "IndexedFieldURI", "FieldIndex");
    indexed_fields.iter().any(|value| *value == base_field_uri)
        && field_indexes.iter().any(|value| *value == field_index)
}
