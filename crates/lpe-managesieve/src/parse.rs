use anyhow::{bail, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

#[derive(Debug)]
pub struct Request {
    pub command: String,
    pub arguments: Vec<Argument>,
}

#[derive(Debug, Clone)]
pub enum Argument {
    Atom(String),
    String(String),
    Literal(String),
}

pub fn single_string_arg(arguments: &[Argument]) -> Result<String> {
    if arguments.len() != 1 {
        bail!("expected exactly one string argument");
    }
    as_string(&arguments[0])
}

pub fn as_string(argument: &Argument) -> Result<String> {
    match argument {
        Argument::Atom(value) | Argument::String(value) | Argument::Literal(value) => {
            Ok(value.clone())
        }
    }
}

pub async fn read_request<R: AsyncBufReadExt + AsyncReadExt + Unpin>(
    reader: &mut R,
) -> Result<Option<Request>> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(None);
    }
    let line = line.trim_end_matches(['\r', '\n']);
    if line.is_empty() {
        return Ok(None);
    }
    let (mut command, mut arguments, literal_len) = parse_request_line(line)?;
    if let Some(literal_len) = literal_len {
        let mut bytes = vec![0; literal_len];
        reader.read_exact(&mut bytes).await?;
        let mut crlf = [0_u8; 2];
        reader.read_exact(&mut crlf).await?;
        arguments.push(Argument::Literal(String::from_utf8(bytes)?));
    }
    Ok(Some(Request {
        command: std::mem::take(&mut command),
        arguments,
    }))
}

pub fn parse_request_line(input: &str) -> Result<(String, Vec<Argument>, Option<usize>)> {
    let mut chars = input.chars().peekable();
    let command = parse_atom(&mut chars)?;
    let mut arguments = Vec::new();
    let mut literal_len = None;
    loop {
        skip_ws(&mut chars);
        let Some(next) = chars.peek().copied() else {
            break;
        };
        match next {
            '"' => arguments.push(Argument::String(parse_quoted(&mut chars)?)),
            '{' => {
                literal_len = Some(parse_literal_marker(&mut chars)?);
                break;
            }
            _ => arguments.push(Argument::Atom(parse_atom(&mut chars)?)),
        }
    }
    Ok((command, arguments, literal_len))
}

fn parse_atom<I>(chars: &mut std::iter::Peekable<I>) -> Result<String>
where
    I: Iterator<Item = char>,
{
    let mut value = String::new();
    while let Some(next) = chars.peek().copied() {
        if next.is_whitespace() {
            break;
        }
        if matches!(next, '"' | '{' | '}') {
            break;
        }
        value.push(next);
        chars.next();
    }
    if value.is_empty() {
        bail!("expected atom");
    }
    Ok(value)
}

fn parse_quoted<I>(chars: &mut std::iter::Peekable<I>) -> Result<String>
where
    I: Iterator<Item = char>,
{
    let mut value = String::new();
    if chars.next() != Some('"') {
        bail!("expected quoted string");
    }
    let mut escaped = false;
    for next in chars.by_ref() {
        if escaped {
            value.push(next);
            escaped = false;
            continue;
        }
        match next {
            '\\' => escaped = true,
            '"' => return Ok(value),
            other => value.push(other),
        }
    }
    bail!("unterminated quoted string")
}

fn parse_literal_marker<I>(chars: &mut std::iter::Peekable<I>) -> Result<usize>
where
    I: Iterator<Item = char>,
{
    if chars.next() != Some('{') {
        bail!("expected literal marker");
    }
    let mut digits = String::new();
    while let Some(next) = chars.peek().copied() {
        if next.is_ascii_digit() {
            digits.push(next);
            chars.next();
        } else {
            break;
        }
    }
    if chars.next() != Some('+') || chars.next() != Some('}') {
        bail!("only non-synchronizing literals are supported");
    }
    digits.parse::<usize>().map_err(Into::into)
}

fn skip_ws<I>(chars: &mut std::iter::Peekable<I>)
where
    I: Iterator<Item = char>,
{
    while matches!(chars.peek(), Some(value) if value.is_whitespace()) {
        chars.next();
    }
}
