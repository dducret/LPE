// [MS-OXRTFCP] sections 2.1.2 through 2.2.3 define the compressed-RTF
// container, CRC, initial dictionary, and LZFu decompression algorithm.

const RTF_HEADER_BYTES: usize = 16;
const RTF_COMPRESSED_MAGIC: u32 = 0x7546_5A4C;
const RTF_UNCOMPRESSED_MAGIC: u32 = 0x414C_454D;
const RTF_DICTIONARY_BYTES: usize = 4096;
const MAX_DECOMPRESSED_RTF_BYTES: usize = 16 * 1024 * 1024;
const MAX_RTF_GROUP_DEPTH: usize = 256;

const INITIAL_RTF_DICTIONARY: &[u8; 207] = b"{\\rtf1\\ansi\\mac\\deff0\\deftab720{\\fonttbl;}{\\f0\\fnil \\froman \\fswiss \\fmodern \\fscript \\fdecor MS Sans SerifSymbolArialTimes New RomanCourier{\\colortbl\\red0\\green0\\blue0\r\n\\par \\pard\\plain\\f0\\fs20\\b\\i\\u\\tab\\tx";

pub(super) fn plain_text_from_rtf_container(value: &[u8]) -> Option<String> {
    let rtf = decompress_rtf_container(value)?;
    plain_text_from_rtf(&rtf)
}

fn decompress_rtf_container(value: &[u8]) -> Option<Vec<u8>> {
    if value.len() < RTF_HEADER_BYTES {
        return None;
    }

    let compressed_size = read_u32(value, 0)?;
    let raw_size = usize::try_from(read_u32(value, 4)?).ok()?;
    let compression_type = read_u32(value, 8)?;
    let expected_crc = read_u32(value, 12)?;
    let container_size = usize::try_from(compressed_size).ok()?.checked_add(4)?;
    if compressed_size < 12
        || container_size != value.len()
        || raw_size > MAX_DECOMPRESSED_RTF_BYTES
    {
        return None;
    }
    let contents = &value[RTF_HEADER_BYTES..];

    match compression_type {
        RTF_UNCOMPRESSED_MAGIC => {
            // [MS-OXRTFCP] section 2.2.3.1 permits a reader to consume exactly
            // RAWSIZE bytes and forbids validating CRC for this container type.
            Some(contents.get(..raw_size)?.to_vec())
        }
        RTF_COMPRESSED_MAGIC => {
            if rtf_crc(contents) != expected_crc {
                return None;
            }
            decompress_lzfu(contents, raw_size)
        }
        _ => None,
    }
}

fn read_u32(value: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        value.get(offset..offset.checked_add(4)?)?.try_into().ok()?,
    ))
}

fn rtf_crc(contents: &[u8]) -> u32 {
    let mut crc = 0_u32;
    for byte in contents {
        let mut table_value = (crc ^ u32::from(*byte)) & 0xFF;
        for _ in 0..8 {
            table_value = if table_value & 1 == 0 {
                table_value >> 1
            } else {
                (table_value >> 1) ^ 0xEDB8_8320
            };
        }
        crc = table_value ^ (crc >> 8);
    }
    crc
}

fn decompress_lzfu(contents: &[u8], raw_size: usize) -> Option<Vec<u8>> {
    let mut dictionary = [0_u8; RTF_DICTIONARY_BYTES];
    dictionary[..INITIAL_RTF_DICTIONARY.len()].copy_from_slice(INITIAL_RTF_DICTIONARY);
    let mut dictionary_write = INITIAL_RTF_DICTIONARY.len();
    let mut dictionary_end = INITIAL_RTF_DICTIONARY.len();
    let mut cursor = 0_usize;
    let mut output = Vec::with_capacity(raw_size);

    while cursor < contents.len() {
        let control = *contents.get(cursor)?;
        cursor += 1;
        for bit in 0..8 {
            if control & (1 << bit) == 0 {
                let literal = *contents.get(cursor)?;
                cursor += 1;
                if output.len() >= raw_size {
                    return None;
                }
                output.push(literal);
                dictionary[dictionary_write] = literal;
                dictionary_write = (dictionary_write + 1) & (RTF_DICTIONARY_BYTES - 1);
                dictionary_end = (dictionary_end + 1).min(RTF_DICTIONARY_BYTES);
                continue;
            }

            let reference = u16::from_be_bytes([
                *contents.get(cursor)?,
                *contents.get(cursor.checked_add(1)?)?,
            ]);
            cursor += 2;
            let mut dictionary_read = usize::from(reference >> 4);
            if dictionary_read == dictionary_write {
                return (output.len() == raw_size).then_some(output);
            }
            if dictionary_end < RTF_DICTIONARY_BYTES && dictionary_read >= dictionary_end {
                return None;
            }

            let match_length = usize::from(reference & 0x000F) + 2;
            for _ in 0..match_length {
                if output.len() >= raw_size {
                    return None;
                }
                let byte = dictionary[dictionary_read];
                dictionary_read = (dictionary_read + 1) & (RTF_DICTIONARY_BYTES - 1);
                output.push(byte);
                dictionary[dictionary_write] = byte;
                dictionary_write = (dictionary_write + 1) & (RTF_DICTIONARY_BYTES - 1);
                dictionary_end = (dictionary_end + 1).min(RTF_DICTIONARY_BYTES);
            }
        }
    }

    None
}

#[derive(Clone, Copy)]
struct RtfTextState {
    ignored: bool,
    hidden: bool,
    unicode_fallback_bytes: usize,
}

impl Default for RtfTextState {
    fn default() -> Self {
        Self {
            ignored: false,
            hidden: false,
            unicode_fallback_bytes: 1,
        }
    }
}

fn plain_text_from_rtf(rtf: &[u8]) -> Option<String> {
    if !rtf.starts_with(b"{\\rtf") {
        return None;
    }

    let mut states = vec![RtfTextState::default()];
    let mut output = String::new();
    let mut pending_high_surrogate = None;
    let mut fallback_bytes_to_skip = 0_usize;
    let mut cursor = 0_usize;

    while cursor < rtf.len() {
        match rtf[cursor] {
            b'{' => {
                if states.len().saturating_sub(1) >= MAX_RTF_GROUP_DEPTH {
                    return None;
                }
                states.push(*states.last()?);
                cursor += 1;
            }
            b'}' => {
                if states.len() == 1 {
                    return None;
                }
                states.pop();
                cursor += 1;
            }
            b'\\' => {
                cursor += 1;
                parse_rtf_control(
                    rtf,
                    &mut cursor,
                    states.last_mut()?,
                    &mut output,
                    &mut pending_high_surrogate,
                    &mut fallback_bytes_to_skip,
                )?;
            }
            b'\r' | b'\n' => cursor += 1,
            byte => {
                cursor += 1;
                if fallback_bytes_to_skip > 0 {
                    fallback_bytes_to_skip -= 1;
                } else if !states.last()?.ignored && !states.last()?.hidden {
                    push_text_char(
                        &mut output,
                        &mut pending_high_surrogate,
                        windows_1252_char(byte),
                    );
                }
            }
        }
    }

    if states.len() != 1 {
        return None;
    }
    if pending_high_surrogate.take().is_some() {
        output.push(char::REPLACEMENT_CHARACTER);
    }
    normalize_rtf_text(&output)
}

fn parse_rtf_control(
    rtf: &[u8],
    cursor: &mut usize,
    state: &mut RtfTextState,
    output: &mut String,
    pending_high_surrogate: &mut Option<u16>,
    fallback_bytes_to_skip: &mut usize,
) -> Option<()> {
    let control = *rtf.get(*cursor)?;
    match control {
        b'\\' | b'{' | b'}' => {
            *cursor += 1;
            push_rtf_byte(
                control,
                state,
                output,
                pending_high_surrogate,
                fallback_bytes_to_skip,
            );
            return Some(());
        }
        b'\'' => {
            let high = hex_digit(*rtf.get(cursor.checked_add(1)?)?)?;
            let low = hex_digit(*rtf.get(cursor.checked_add(2)?)?)?;
            *cursor += 3;
            push_rtf_byte(
                (high << 4) | low,
                state,
                output,
                pending_high_surrogate,
                fallback_bytes_to_skip,
            );
            return Some(());
        }
        b'*' => {
            state.ignored = true;
            *cursor += 1;
            return Some(());
        }
        b'~' => {
            *cursor += 1;
            push_rtf_char(
                ' ',
                state,
                output,
                pending_high_surrogate,
                fallback_bytes_to_skip,
            );
            return Some(());
        }
        b'_' => {
            *cursor += 1;
            push_rtf_char(
                '-',
                state,
                output,
                pending_high_surrogate,
                fallback_bytes_to_skip,
            );
            return Some(());
        }
        b'-' => {
            *cursor += 1;
            return Some(());
        }
        b'\r' | b'\n' => {
            *cursor += 1;
            if control == b'\r' && rtf.get(*cursor) == Some(&b'\n') {
                *cursor += 1;
            }
            return Some(());
        }
        _ => {}
    }

    let word_start = *cursor;
    while rtf.get(*cursor).is_some_and(u8::is_ascii_alphabetic) {
        *cursor += 1;
    }
    if *cursor == word_start {
        *cursor += 1;
        return Some(());
    }
    let word = std::str::from_utf8(rtf.get(word_start..*cursor)?).ok()?;

    let negative = if rtf.get(*cursor) == Some(&b'-') {
        *cursor += 1;
        true
    } else {
        false
    };
    let number_start = *cursor;
    while rtf.get(*cursor).is_some_and(u8::is_ascii_digit) {
        *cursor += 1;
    }
    let parameter = if *cursor == number_start {
        None
    } else {
        let magnitude = std::str::from_utf8(rtf.get(number_start..*cursor)?)
            .ok()?
            .parse::<i32>()
            .ok()?;
        Some(if negative { -magnitude } else { magnitude })
    };
    if rtf.get(*cursor) == Some(&b' ') {
        *cursor += 1;
    }

    if word == "bin" {
        let byte_count = usize::try_from(parameter?).ok()?;
        *cursor = cursor.checked_add(byte_count)?;
        rtf.get(..*cursor)?;
        return Some(());
    }
    if is_ignored_destination(word) {
        state.ignored = true;
    }
    if state.ignored {
        return Some(());
    }

    match word {
        "uc" => state.unicode_fallback_bytes = usize::try_from(parameter?).ok()?,
        "v" => state.hidden = parameter.unwrap_or(1) != 0,
        "plain" => state.hidden = false,
        "u" => {
            let value = parameter?;
            let code_unit = if (-32_768..=32_767).contains(&value) {
                value as i16 as u16
            } else {
                u16::try_from(value).ok()?
            };
            if !state.hidden {
                push_unicode_code_unit(output, pending_high_surrogate, code_unit);
            }
            *fallback_bytes_to_skip = state.unicode_fallback_bytes;
        }
        _ if state.hidden => {}
        "par" | "line" => push_text_char(output, pending_high_surrogate, '\n'),
        "tab" => push_text_char(output, pending_high_surrogate, '\t'),
        "emdash" => push_text_char(output, pending_high_surrogate, '\u{2014}'),
        "endash" => push_text_char(output, pending_high_surrogate, '\u{2013}'),
        "bullet" => push_text_char(output, pending_high_surrogate, '\u{2022}'),
        "lquote" | "rquote" => push_text_char(output, pending_high_surrogate, '\''),
        "ldblquote" | "rdblquote" => push_text_char(output, pending_high_surrogate, '"'),
        _ => {}
    }
    Some(())
}

fn is_ignored_destination(word: &str) -> bool {
    matches!(
        word,
        "colortbl"
            | "datastore"
            | "filetbl"
            | "fldinst"
            | "fonttbl"
            | "footer"
            | "footerf"
            | "footerl"
            | "footerr"
            | "generator"
            | "header"
            | "headerf"
            | "headerl"
            | "headerr"
            | "info"
            | "listoverridetable"
            | "listtable"
            | "nonshppict"
            | "object"
            | "objdata"
            | "pict"
            | "pntext"
            | "revtbl"
            | "shp"
            | "shppict"
            | "stylesheet"
            | "xmlnstbl"
    )
}

fn push_rtf_byte(
    byte: u8,
    state: &RtfTextState,
    output: &mut String,
    pending_high_surrogate: &mut Option<u16>,
    fallback_bytes_to_skip: &mut usize,
) {
    push_rtf_char(
        windows_1252_char(byte),
        state,
        output,
        pending_high_surrogate,
        fallback_bytes_to_skip,
    );
}

fn push_rtf_char(
    ch: char,
    state: &RtfTextState,
    output: &mut String,
    pending_high_surrogate: &mut Option<u16>,
    fallback_bytes_to_skip: &mut usize,
) {
    if *fallback_bytes_to_skip > 0 {
        *fallback_bytes_to_skip -= 1;
    } else if !state.ignored && !state.hidden {
        push_text_char(output, pending_high_surrogate, ch);
    }
}

fn push_unicode_code_unit(
    output: &mut String,
    pending_high_surrogate: &mut Option<u16>,
    code_unit: u16,
) {
    match code_unit {
        0xD800..=0xDBFF => {
            if pending_high_surrogate.replace(code_unit).is_some() {
                output.push(char::REPLACEMENT_CHARACTER);
            }
        }
        0xDC00..=0xDFFF => {
            let Some(high) = pending_high_surrogate.take() else {
                output.push(char::REPLACEMENT_CHARACTER);
                return;
            };
            let scalar =
                0x1_0000 + ((u32::from(high) - 0xD800) << 10) + (u32::from(code_unit) - 0xDC00);
            output.push(char::from_u32(scalar).unwrap_or(char::REPLACEMENT_CHARACTER));
        }
        _ => {
            let ch = char::from_u32(u32::from(code_unit)).unwrap_or(char::REPLACEMENT_CHARACTER);
            push_text_char(output, pending_high_surrogate, ch);
        }
    }
}

fn push_text_char(output: &mut String, pending_high_surrogate: &mut Option<u16>, ch: char) {
    if ch.is_control() && !matches!(ch, '\n' | '\t') {
        if pending_high_surrogate.take().is_some() {
            output.push(char::REPLACEMENT_CHARACTER);
        }
        return;
    }
    if pending_high_surrogate.take().is_some() {
        output.push(char::REPLACEMENT_CHARACTER);
    }
    output.push(ch);
}

fn windows_1252_char(byte: u8) -> char {
    match byte {
        0x80 => '\u{20AC}',
        0x82 => '\u{201A}',
        0x83 => '\u{0192}',
        0x84 => '\u{201E}',
        0x85 => '\u{2026}',
        0x86 => '\u{2020}',
        0x87 => '\u{2021}',
        0x88 => '\u{02C6}',
        0x89 => '\u{2030}',
        0x8A => '\u{0160}',
        0x8B => '\u{2039}',
        0x8C => '\u{0152}',
        0x8E => '\u{017D}',
        0x91 => '\u{2018}',
        0x92 => '\u{2019}',
        0x93 => '\u{201C}',
        0x94 => '\u{201D}',
        0x95 => '\u{2022}',
        0x96 => '\u{2013}',
        0x97 => '\u{2014}',
        0x98 => '\u{02DC}',
        0x99 => '\u{2122}',
        0x9A => '\u{0161}',
        0x9B => '\u{203A}',
        0x9C => '\u{0153}',
        0x9E => '\u{017E}',
        0x9F => '\u{0178}',
        0x81 | 0x8D | 0x8F | 0x90 | 0x9D => char::REPLACEMENT_CHARACTER,
        _ => char::from_u32(u32::from(byte)).unwrap_or(char::REPLACEMENT_CHARACTER),
    }
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn normalize_rtf_text(value: &str) -> Option<String> {
    let mut normalized = value
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n");
    while normalized.contains("\n\n\n") {
        normalized = normalized.replace("\n\n\n", "\n\n");
    }
    let normalized = normalized.trim().to_string();
    (!normalized.is_empty()).then_some(normalized)
}

#[cfg(test)]
pub(crate) const OUTLOOK_SYNC_LOG_RTF_BASE64: &str = "8wQAAGESAABMWkZ1Zp++Mj8ACgEDAfcCpANjAgBjaBEKwHNldALRcHJx/DIgBxMCgwBQA9QP5A9ILxBGApEI7wn3OxOiMTJCOBP/ZDI1NRVMfRELRnMxOAqQdWMxAiAYEDozMDo0NrkGAHluD2ADYAMAegSQPCBWBJAAkAIgGIA2LrQwLgHQMhUhCrEKGJ9bGaALgGcF0AtwbAbgeKQgJxDRIHQHkHQCsXcdQRsvHDhICJEKwA9geU0eOjcDMAGRCiAhsDQKIAIQbASBKHMpIJB1cGRhHaBkIAuAyiACIGwLgGUgHcAFsIplHjo4G+1GYXYFsEZpHaEkHERvdyNgb7xhZBySA1IjsASQdhnBBCdoAkBwczovLwMAwAMQLmwtcC1lBi4PYClBcGkvZW1Ac21kYi8/HNVJ9GQ9HaJAKaYeKyTeKFU/D2EcoAeRIyEiBB1ESW7/HREd7ycvKD8pTypfK28sfashZhhwdgiQdyJhLwIQ/HJtImIyAAEAIwAj0CNA/wEgI3MiBCyPLZ8ury+1CFD7AjAA0HQRYTCfMa8yvzPPvzTfNe867zv/PQ8dREQgIP8BgD7vP/9BD0IfQy9EP0VPh0ZfR28vl1Rhc2tJX79Kb0t/TI9Nn06vT7w5UK/rUb8veUoIYW4HQFPPWvD/VP9WD1cfWC9ZP1pPW19cb/kdRE5vJjJeX19vYH9hjz9in2OvZL9lz2bfL7VTdf5ncYEi4j5/aT9qT2tfbG//bX9uj2+fcK9xvxkxMBAEEP8KUHPPdN9173b/eA95H3ovB3s/fE9dSHVuayBF/i2B0n6ff6+Av4HPgt+D70+E/4YPhx8vplJTBfBG/QngZH6Pil+Lb4x/jY+On7+Pr5C/kc8vlwcQD2BplmB/k/+VD5Yfly+YP5lPHio1/xJgm0+cXz3bOhFzr6Vzn6//oL+hz6Lfo++k/6YPpx8dRPhMb2MHQCXBAxAIcH5//6oPqx+sL60/rk+vX7BvsX//chm2VLOPtJ+1r7a/t8+433+577ryfcS7n7yvPhfBcXP7ItAaMkGo4BoyBmACQByR/77/xiHAP8FPwl/Db8R/xY9nxp/Hrx1EUXWo0IjQU/0doHDKT8tfzG/Nf86Pz5//0K/Rv9LP098GYAIwMBAdoP/akNYf1y/YP9lP2l/bb7przxAw3W/ef0hpZWwPsCLx/+B/56Xh/+MP5B/lL+Y/50/9HDhWOHEmWuyv7b/uz+/f//Dv8f71D/Yf9y/4P/lP8iv+VQtQ+3U5wfw//U/+X/9vOx5CExBmEDC6xSHgRXL7DvAZ0HMcCzhiONMXiTewBwZfN7Q3xFs4MDA0IDAxMEYtuyAxLXUL9zMawF0JTwpfC2ZUmmgjoGMjcOAyb3AgEe3Jk2Yc4XMgLg2fDq8LZjZN1QAZcHM58OBQRXj/6WQwETjiyYTVQCPjEh8TL/U3xEYHoW0j4SMRFejJsd50njDpwBGCvsEsEGLVEYMa8COgVVJMIGLq4L37MDoW3xfvC2YClncgMFwubRSmA7D8AC9+UHCicCPgdC9wJUBkvtBX+5C+0MnwL13gdPtgb4ZrDWAM0F91cy5TgKRwP8lQcj0L9WYMet8kgA1RHR8efDgRNThfOWj/+1A6PwcF+yApsB3WOEAG5/8UnynAOfY5Yb7R6cAFACLwZyWvK7w3wzBYC/YvD30BMwA=";

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};

    #[test]
    fn outlook_sync_log_lzfu_body_decompresses_and_extracts_plain_text() {
        let container = BASE64_STANDARD
            .decode(OUTLOOK_SYNC_LOG_RTF_BASE64)
            .expect("captured RTF fixture");
        let raw = decompress_rtf_container(&container).expect("valid LZFu container");
        assert_eq!(raw.len(), 4_705);

        let text = plain_text_from_rtf_container(&container).expect("readable RTF text");
        assert!(text.starts_with("18:30:46 Synchronizer Version 16.0.20228"));
        assert!(text.contains("18:30:54 Error synchronizing view/form"));
        assert!(text.contains("[8004010F-501-8004010F-320]"));
        assert!(text.contains("5 view(s)/form(s) added to online folder"));
        assert!(!text.contains("fonttbl"));
    }

    #[test]
    fn compressed_rtf_rejects_crc_and_declared_size_corruption() {
        let container = BASE64_STANDARD
            .decode(OUTLOOK_SYNC_LOG_RTF_BASE64)
            .expect("captured RTF fixture");

        let mut bad_crc = container.clone();
        bad_crc[16] ^= 0x01;
        assert!(decompress_rtf_container(&bad_crc).is_none());

        let mut oversized = container;
        oversized[4..8].copy_from_slice(&((MAX_DECOMPRESSED_RTF_BYTES as u32) + 1).to_le_bytes());
        assert!(decompress_rtf_container(&oversized).is_none());
    }

    #[test]
    fn uncompressed_rtf_extracts_escaped_and_unicode_text() {
        let raw = br"{\rtf1\ansi\uc1 Plain \{text\}\par Caf\'e9 \u8364?}";
        let mut container = Vec::new();
        container.extend_from_slice(&(u32::try_from(raw.len()).unwrap() + 12).to_le_bytes());
        container.extend_from_slice(&u32::try_from(raw.len()).unwrap().to_le_bytes());
        container.extend_from_slice(&RTF_UNCOMPRESSED_MAGIC.to_le_bytes());
        container.extend_from_slice(&0_u32.to_le_bytes());
        container.extend_from_slice(raw);

        assert_eq!(
            plain_text_from_rtf_container(&container).as_deref(),
            Some("Plain {text}\nCafé €")
        );
    }

    #[test]
    fn rtf_text_extraction_rejects_excessive_group_nesting() {
        fn nested_rtf(group_depth: usize) -> Vec<u8> {
            let mut rtf = br"{\rtf1 ".to_vec();
            rtf.extend(std::iter::repeat(b'{').take(group_depth - 1));
            rtf.extend_from_slice(b"text");
            rtf.extend(std::iter::repeat(b'}').take(group_depth));
            rtf
        }

        assert_eq!(
            plain_text_from_rtf(&nested_rtf(MAX_RTF_GROUP_DEPTH)).as_deref(),
            Some("text")
        );
        assert!(plain_text_from_rtf(&nested_rtf(MAX_RTF_GROUP_DEPTH + 1)).is_none());
    }

    #[test]
    fn rtf_text_extraction_drops_storage_unsafe_control_characters() {
        let rtf = b"{\\rtf1\\uc1 A\0B\\'00C\\'01D\\u0?\\u1?\\tab E\\par F}";
        let text = plain_text_from_rtf(rtf).expect("readable RTF text");

        assert_eq!(text, "ABCD\tE\nF");
        assert!(!text.contains('\0'));
        assert!(text
            .chars()
            .all(|ch| !ch.is_control() || matches!(ch, '\n' | '\t')));
    }
}
