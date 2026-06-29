use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};

use super::super::*;

pub(in crate::service) fn get_user_configuration_response(
    configuration: &EwsUserConfiguration,
    request: &str,
) -> String {
    let properties = requested_user_configuration_properties(request);
    let dictionary = if properties.dictionary {
        ews_user_configuration_dictionary_xml(&configuration.dictionary_json)
    } else {
        String::new()
    };
    let xml_data = if properties.xml_data {
        configuration
            .xml_payload
            .as_ref()
            .map(|value| format!("<t:XmlData>{}</t:XmlData>", escape_xml(value)))
            .unwrap_or_default()
    } else {
        String::new()
    };
    let binary_data = if properties.binary_data {
        configuration
            .binary_payload
            .as_ref()
            .map(|value| {
                format!(
                    "<t:BinaryData>{}</t:BinaryData>",
                    BASE64_STANDARD.encode(value)
                )
            })
            .unwrap_or_default()
    } else {
        String::new()
    };
    format!(
        concat!(
            "<m:GetUserConfigurationResponse>",
            "<m:ResponseMessages>",
            "<m:GetUserConfigurationResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:UserConfiguration>",
            "<t:UserConfigurationName Name=\"{name}\"/>",
            "<t:ItemId Id=\"user-configuration:{id}\" ChangeKey=\"{change_key}\"/>",
            "{dictionary}",
            "{xml_data}",
            "{binary_data}",
            "</m:UserConfiguration>",
            "</m:GetUserConfigurationResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetUserConfigurationResponse>"
        ),
        name = escape_xml(&configuration.config_name),
        id = configuration.id,
        change_key = configuration.modseq,
        dictionary = dictionary,
        xml_data = xml_data,
        binary_data = binary_data,
    )
}

#[derive(Debug, Clone, Copy)]
struct RequestedUserConfigurationProperties {
    dictionary: bool,
    xml_data: bool,
    binary_data: bool,
}

fn requested_user_configuration_properties(request: &str) -> RequestedUserConfigurationProperties {
    let values = element_contents(request, "UserConfigurationProperties")
        .into_iter()
        .map(xml_text)
        .collect::<Vec<_>>();
    if values.is_empty() || values.iter().any(|value| value.eq_ignore_ascii_case("All")) {
        return RequestedUserConfigurationProperties {
            dictionary: true,
            xml_data: true,
            binary_data: true,
        };
    }
    RequestedUserConfigurationProperties {
        dictionary: values
            .iter()
            .any(|value| value.eq_ignore_ascii_case("Dictionary")),
        xml_data: values
            .iter()
            .any(|value| value.eq_ignore_ascii_case("XmlData")),
        binary_data: values
            .iter()
            .any(|value| value.eq_ignore_ascii_case("BinaryData")),
    }
}

fn ews_user_configuration_dictionary_xml(dictionary: &serde_json::Value) -> String {
    let Some(object) = dictionary.as_object() else {
        return "<t:Dictionary/>".to_string();
    };
    if object.is_empty() {
        return "<t:Dictionary/>".to_string();
    }
    let entries = object
        .iter()
        .map(|(key, value)| {
            let value = value.as_str().unwrap_or_default();
            format!(
                concat!(
                    "<t:DictionaryEntry>",
                    "<t:DictionaryKey><t:Type>String</t:Type><t:Value>{key}</t:Value></t:DictionaryKey>",
                    "<t:DictionaryValue><t:Type>String</t:Type><t:Value>{value}</t:Value></t:DictionaryValue>",
                    "</t:DictionaryEntry>"
                ),
                key = escape_xml(key),
                value = escape_xml(value),
            )
        })
        .collect::<String>();
    format!("<t:Dictionary>{entries}</t:Dictionary>")
}
