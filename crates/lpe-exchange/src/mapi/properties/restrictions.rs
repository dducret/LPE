use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct MapiSortOrder {
    pub(in crate::mapi) property_tag: u32,
    pub(in crate::mapi) order: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) enum MapiRestriction {
    InvalidTableRestriction,
    And(Vec<MapiRestriction>),
    Or(Vec<MapiRestriction>),
    Not(Box<MapiRestriction>),
    Content {
        property_tag: u32,
        value: String,
        fuzzy_level_low: u16,
        fuzzy_level_high: u16,
    },
    Property {
        relop: u8,
        property_tag: u32,
        value: MapiValue,
    },
    CompareProperties {
        relop: u8,
        left_property_tag: u32,
        right_property_tag: u32,
    },
    Bitmask {
        property_tag: u32,
        mask: u32,
        must_be_nonzero: bool,
    },
    Size {
        relop: u8,
        property_tag: u32,
        size: u32,
    },
    Exist {
        property_tag: u32,
    },
    Count {
        count: u32,
        child: Box<MapiRestriction>,
    },
    SubObject {
        subobject: u32,
        child: Box<MapiRestriction>,
    },
}
