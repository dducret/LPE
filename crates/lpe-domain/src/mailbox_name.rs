use std::{error::Error, fmt};

use unicode_normalization::UnicodeNormalization;

pub const MAILBOX_HIERARCHY_DELIMITER: char = '/';
const MAX_SEGMENT_CHARS: usize = 64;
const MAX_PATH_SEGMENTS: usize = 16;
const MAX_PATH_BYTES: usize = 255;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailboxDisplayName(String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailboxSegment(MailboxDisplayName);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailboxPath {
    segments: Vec<MailboxSegment>,
    display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailboxCanonicalKey {
    equality: String,
    skeleton: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MailboxNameError {
    Empty,
    EmptySegment,
    TooLong,
    TooDeep,
    LeadingOrTrailingWhitespace,
    ContainsDelimiter,
    ContainsControl,
    ContainsUnsafeInvisible,
    ReservedName,
}

impl MailboxDisplayName {
    pub fn new(value: impl AsRef<str>) -> Result<Self, MailboxNameError> {
        Self::validate(value.as_ref(), ReservedNamePolicy::Reject)
    }

    pub fn system(value: impl AsRef<str>) -> Result<Self, MailboxNameError> {
        Self::validate(value.as_ref(), ReservedNamePolicy::Allow)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn canonical_key(&self) -> MailboxCanonicalKey {
        MailboxCanonicalKey::for_display_name(&self.0)
    }

    fn validate(
        value: &str,
        reserved_policy: ReservedNamePolicy,
    ) -> Result<Self, MailboxNameError> {
        validate_raw_segment(value)?;
        let normalized = value.nfc().collect::<String>();
        validate_normalized_segment(&normalized)?;
        if matches!(reserved_policy, ReservedNamePolicy::Reject)
            && MailboxNamePolicy::is_reserved_key(&MailboxCanonicalKey::for_display_name(
                &normalized,
            ))
        {
            return Err(MailboxNameError::ReservedName);
        }
        Ok(Self(normalized))
    }
}

impl MailboxSegment {
    pub fn new(value: impl AsRef<str>) -> Result<Self, MailboxNameError> {
        MailboxDisplayName::new(value).map(Self)
    }

    pub fn system(value: impl AsRef<str>) -> Result<Self, MailboxNameError> {
        MailboxDisplayName::system(value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn display_name(&self) -> &MailboxDisplayName {
        &self.0
    }

    pub fn into_display_name(self) -> MailboxDisplayName {
        self.0
    }
}

impl MailboxPath {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, MailboxNameError> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(MailboxNameError::Empty);
        }
        if value.len() > MAX_PATH_BYTES {
            return Err(MailboxNameError::TooLong);
        }

        let mut segments = Vec::new();
        for raw_segment in value.split(MAILBOX_HIERARCHY_DELIMITER) {
            if raw_segment.is_empty() {
                return Err(MailboxNameError::EmptySegment);
            }
            segments.push(MailboxSegment::new(raw_segment)?);
        }
        if segments.len() > MAX_PATH_SEGMENTS {
            return Err(MailboxNameError::TooDeep);
        }

        let display_name = segments
            .iter()
            .map(MailboxSegment::as_str)
            .collect::<Vec<_>>()
            .join("/");
        Ok(Self {
            segments,
            display_name,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.display_name
    }

    pub fn segments(&self) -> &[MailboxSegment] {
        &self.segments
    }

    pub fn into_string(self) -> String {
        self.display_name
    }
}

impl MailboxCanonicalKey {
    pub fn for_display_name(value: &str) -> Self {
        let normalized = value.nfc().collect::<String>();
        let equality = fold_for_comparison(&normalized);
        let skeleton = fold_for_comparison(&normalized.nfkc().collect::<String>())
            .chars()
            .map(confusable_skeleton_char)
            .collect();
        Self { equality, skeleton }
    }

    pub fn as_str(&self) -> &str {
        &self.equality
    }

    pub fn skeleton(&self) -> &str {
        &self.skeleton
    }

    pub fn collides_with(&self, other: &Self) -> bool {
        self.equality == other.equality || self.skeleton == other.skeleton
    }
}

pub struct MailboxNamePolicy;

impl MailboxNamePolicy {
    pub fn canonical_key(value: &str) -> MailboxCanonicalKey {
        MailboxCanonicalKey::for_display_name(value)
    }

    pub fn system_role_for_display_name(value: &str) -> Option<&'static str> {
        let key = MailboxCanonicalKey::for_display_name(value);
        RESERVED_MAILBOX_NAMES
            .iter()
            .find_map(|reserved| reserved.key.matches(&key).then_some(reserved.role))
    }

    pub fn canonical_system_display_name(role: &str) -> Option<&'static str> {
        match role {
            "inbox" => Some("Inbox"),
            "drafts" => Some("Drafts"),
            "sent" => Some("Sent"),
            "trash" => Some("Deleted"),
            "junk" => Some("Junk"),
            "archive" => Some("Archive"),
            _ => None,
        }
    }

    fn is_reserved_key(key: &MailboxCanonicalKey) -> bool {
        RESERVED_MAILBOX_NAMES
            .iter()
            .any(|reserved| reserved.key.matches(key))
    }
}

impl fmt::Display for MailboxNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "mailbox name is required",
            Self::EmptySegment => "mailbox path contains an empty segment",
            Self::TooLong => "mailbox name is too long",
            Self::TooDeep => "mailbox path is too deep",
            Self::LeadingOrTrailingWhitespace => {
                "mailbox name must not start or end with whitespace"
            }
            Self::ContainsDelimiter => "mailbox name segment contains the hierarchy delimiter",
            Self::ContainsControl => "mailbox name contains a control character",
            Self::ContainsUnsafeInvisible => "mailbox name contains an unsafe invisible character",
            Self::ReservedName => "mailbox name is reserved",
        })
    }
}

impl Error for MailboxNameError {}

impl fmt::Display for MailboxDisplayName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl fmt::Display for MailboxSegment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl fmt::Display for MailboxPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy)]
enum ReservedNamePolicy {
    Allow,
    Reject,
}

struct ReservedMailboxName {
    role: &'static str,
    key: ReservedMailboxKey,
}

struct ReservedMailboxKey {
    equality: &'static str,
    skeleton: &'static str,
}

impl ReservedMailboxKey {
    fn matches(&self, key: &MailboxCanonicalKey) -> bool {
        self.equality == key.as_str() || self.skeleton == key.skeleton()
    }
}

const RESERVED_MAILBOX_NAMES: &[ReservedMailboxName] = &[
    reserved("inbox", "inbox", "inbox"),
    reserved("drafts", "draft", "draft"),
    reserved("drafts", "drafts", "drafts"),
    reserved("sent", "sent", "sent"),
    reserved("sent", "sent items", "sent items"),
    reserved("sent", "sent messages", "sent messages"),
    reserved("trash", "deleted", "deleted"),
    reserved("trash", "deleted items", "deleted items"),
    reserved("trash", "trash", "trash"),
    reserved("junk", "junk", "junk"),
    reserved("junk", "spam", "spam"),
    reserved("archive", "archive", "archive"),
];

const fn reserved(
    role: &'static str,
    equality: &'static str,
    skeleton: &'static str,
) -> ReservedMailboxName {
    ReservedMailboxName {
        role,
        key: ReservedMailboxKey { equality, skeleton },
    }
}

fn validate_raw_segment(value: &str) -> Result<(), MailboxNameError> {
    if value.is_empty() {
        return Err(MailboxNameError::Empty);
    }
    if value.chars().count() > MAX_SEGMENT_CHARS || value.len() > MAX_PATH_BYTES {
        return Err(MailboxNameError::TooLong);
    }
    if has_ascii_boundary_whitespace(value) {
        return Err(MailboxNameError::LeadingOrTrailingWhitespace);
    }
    if value.contains(MAILBOX_HIERARCHY_DELIMITER) {
        return Err(MailboxNameError::ContainsDelimiter);
    }
    validate_codepoints(value)
}

fn validate_normalized_segment(value: &str) -> Result<(), MailboxNameError> {
    if value.is_empty() {
        return Err(MailboxNameError::Empty);
    }
    if value.chars().count() > MAX_SEGMENT_CHARS || value.len() > MAX_PATH_BYTES {
        return Err(MailboxNameError::TooLong);
    }
    validate_codepoints(value)
}

fn validate_codepoints(value: &str) -> Result<(), MailboxNameError> {
    for ch in value.chars() {
        if ch.is_control() || ch == '\u{7f}' {
            return Err(MailboxNameError::ContainsControl);
        }
        if is_private_use(ch) || is_unsafe_invisible(ch) {
            return Err(MailboxNameError::ContainsUnsafeInvisible);
        }
    }
    Ok(())
}

fn has_ascii_boundary_whitespace(value: &str) -> bool {
    value
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_whitespace)
        || value.as_bytes().last().is_some_and(u8::is_ascii_whitespace)
}

fn is_private_use(ch: char) -> bool {
    matches!(
        ch as u32,
        0xE000..=0xF8FF | 0xF0000..=0xFFFFD | 0x100000..=0x10FFFD
    )
}

fn is_unsafe_invisible(ch: char) -> bool {
    matches!(
        ch as u32,
        0x00AD
            | 0x034F
            | 0x061C
            | 0x115F..=0x1160
            | 0x17B4..=0x17B5
            | 0x180B..=0x180F
            | 0x200B..=0x200F
            | 0x202A..=0x202E
            | 0x2060..=0x206F
            | 0xFE00..=0xFE0F
            | 0xFEFF
            | 0xFFF0..=0xFFF8
            | 0xE0000..=0xE007F
            | 0xE0100..=0xE01EF
    )
}

fn fold_for_comparison(value: &str) -> String {
    value
        .chars()
        .flat_map(|ch| match ch {
            '\u{00df}' | '\u{1e9e}' => "ss".chars().collect::<Vec<_>>(),
            _ => ch.to_lowercase().collect::<Vec<_>>(),
        })
        .collect::<String>()
        .split_ascii_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn confusable_skeleton_char(ch: char) -> char {
    match ch {
        'Α' | 'А' | 'а' | 'ɑ' | 'α' => 'a',
        'Β' | 'В' | 'Ᏼ' | 'β' => 'b',
        'С' | 'с' | 'Ϲ' | 'ϲ' => 'c',
        'ԁ' | 'ⅾ' => 'd',
        'Ε' | 'Е' | 'е' | '℮' => 'e',
        'Ғ' | 'ғ' => 'f',
        'ɡ' | 'ց' => 'g',
        'Η' | 'Н' | 'һ' => 'h',
        'Ι' | 'І' | 'і' | 'Ӏ' | 'ɩ' | 'ı' | 'Ꭵ' => 'i',
        'Ј' | 'ј' | 'ϳ' => 'j',
        'Κ' | 'К' | 'κ' => 'k',
        'Ꮮ' | 'ⅼ' | 'ӏ' => 'l',
        'Μ' | 'М' | 'м' => 'm',
        'Ν' | 'П' | 'ո' | 'п' => 'n',
        'Ο' | 'О' | 'о' | 'ο' | 'Օ' | 'օ' | '0' => 'o',
        'Ρ' | 'Р' | 'р' => 'p',
        'ԛ' => 'q',
        'Ꭱ' | 'ᴦ' => 'r',
        'Ѕ' | 'ѕ' | 'Տ' | 'Ⴝ' => 's',
        'Τ' | 'Т' | 'τ' => 't',
        'υ' | 'ս' => 'u',
        'ν' | 'ѵ' => 'v',
        'ԝ' | 'ա' => 'w',
        'Χ' | 'Х' | 'х' | 'χ' => 'x',
        'Υ' | 'У' | 'у' | 'ү' => 'y',
        'Ζ' | 'Ꮓ' | 'ᴢ' => 'z',
        _ => ch,
    }
}
