use anyhow::{anyhow, bail, Result};
use std::collections::BTreeMap;

const ALLOWED_REQUIREMENTS: &[&str] = &["fileinto", "discard", "redirect", "vacation"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Script {
    pub requirements: Vec<String>,
    pub statements: Vec<Statement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    If {
        branches: Vec<(Test, Vec<Statement>)>,
        else_block: Option<Vec<Statement>>,
    },
    Action(Action),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Test {
    True,
    False,
    Header {
        match_type: MatchType,
        fields: Vec<String>,
        keys: Vec<String>,
    },
    Address {
        match_type: MatchType,
        fields: Vec<String>,
        keys: Vec<String>,
    },
    Envelope {
        match_type: MatchType,
        parts: Vec<String>,
        keys: Vec<String>,
    },
    AllOf(Vec<Test>),
    AnyOf(Vec<Test>),
    Not(Box<Test>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchType {
    Is,
    Contains,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Keep,
    Discard,
    FileInto(String),
    Redirect(String),
    Vacation {
        subject: Option<String>,
        days: u32,
        reason: String,
    },
    Stop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageContext {
    pub envelope_from: String,
    pub envelope_to: String,
    pub headers: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VacationAction {
    pub subject: Option<String>,
    pub days: u32,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionOutcome {
    pub keep: bool,
    pub file_into: Option<String>,
    pub discard: bool,
    pub redirects: Vec<String>,
    pub vacation: Option<VacationAction>,
}

impl Default for ExecutionOutcome {
    fn default() -> Self {
        Self {
            keep: true,
            file_into: None,
            discard: false,
            redirects: Vec::new(),
            vacation: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Identifier(String),
    String(String),
    Number(u32),
    Colon,
    Semicolon,
    Comma,
    LeftParen,
    RightParen,
    LeftBrace,
    RightBrace,
    LeftBracket,
    RightBracket,
}

pub fn parse_script(input: &str) -> Result<Script> {
    let tokens = tokenize(input)?;
    let mut parser = Parser {
        tokens: &tokens,
        position: 0,
    };
    parser.parse_script()
}

pub fn evaluate_script(script: &Script, context: &MessageContext) -> Result<ExecutionOutcome> {
    let mut outcome = ExecutionOutcome::default();
    let mut stopped = false;
    execute_block(&script.statements, context, &mut outcome, &mut stopped)?;
    Ok(outcome)
}

fn execute_block(
    statements: &[Statement],
    context: &MessageContext,
    outcome: &mut ExecutionOutcome,
    stopped: &mut bool,
) -> Result<()> {
    for statement in statements {
        if *stopped {
            break;
        }
        match statement {
            Statement::Action(action) => execute_action(action, outcome, stopped),
            Statement::If {
                branches,
                else_block,
            } => {
                let mut matched = false;
                for (test, block) in branches {
                    if evaluate_test(test, context) {
                        execute_block(block, context, outcome, stopped)?;
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    if let Some(block) = else_block {
                        execute_block(block, context, outcome, stopped)?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn execute_action(action: &Action, outcome: &mut ExecutionOutcome, stopped: &mut bool) {
    match action {
        Action::Keep => {
            if !outcome.discard && outcome.file_into.is_none() {
                outcome.keep = true;
            }
        }
        Action::Discard => {
            outcome.discard = true;
            outcome.keep = false;
            outcome.file_into = None;
        }
        Action::FileInto(mailbox) => {
            if !outcome.discard && outcome.file_into.is_none() {
                outcome.keep = false;
                outcome.file_into = Some(mailbox.clone());
            }
        }
        Action::Redirect(address) => {
            if !outcome
                .redirects
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(address))
            {
                outcome.redirects.push(address.clone());
            }
        }
        Action::Vacation {
            subject,
            days,
            reason,
        } => {
            if outcome.vacation.is_none() {
                outcome.vacation = Some(VacationAction {
                    subject: subject.clone(),
                    days: *days,
                    reason: reason.clone(),
                });
            }
        }
        Action::Stop => {
            *stopped = true;
        }
    }
}

fn evaluate_test(test: &Test, context: &MessageContext) -> bool {
    match test {
        Test::True => true,
        Test::False => false,
        Test::Header {
            match_type,
            fields,
            keys,
        } => fields.iter().any(|field| {
            context
                .headers
                .get(&field.to_lowercase())
                .into_iter()
                .flatten()
                .any(|value| matches_any(value, keys, *match_type))
        }),
        Test::Address {
            match_type,
            fields,
            keys,
        } => fields.iter().any(|field| {
            context
                .headers
                .get(&field.to_lowercase())
                .into_iter()
                .flatten()
                .flat_map(|value| extract_addresses(value))
                .any(|value| matches_any(&value, keys, *match_type))
        }),
        Test::Envelope {
            match_type,
            parts,
            keys,
        } => parts.iter().any(|part| {
            let value = match part.as_str() {
                "from" => context.envelope_from.as_str(),
                "to" => context.envelope_to.as_str(),
                _ => "",
            };
            !value.is_empty() && matches_any(value, keys, *match_type)
        }),
        Test::AllOf(tests) => tests.iter().all(|nested| evaluate_test(nested, context)),
        Test::AnyOf(tests) => tests.iter().any(|nested| evaluate_test(nested, context)),
        Test::Not(nested) => !evaluate_test(nested, context),
    }
}

fn matches_any(value: &str, keys: &[String], match_type: MatchType) -> bool {
    let value = value.trim().to_lowercase();
    keys.iter().any(|key| {
        let key = key.trim().to_lowercase();
        match match_type {
            MatchType::Is => value == key,
            MatchType::Contains => value.contains(&key),
        }
    })
}

fn extract_addresses(value: &str) -> Vec<String> {
    value
        .split(',')
        .filter_map(|part| {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                return None;
            }
            if let (Some(start), Some(end)) = (trimmed.rfind('<'), trimmed.rfind('>')) {
                let address = trimmed[start + 1..end].trim().to_lowercase();
                if !address.is_empty() {
                    return Some(address);
                }
            }
            Some(trimmed.trim_matches('"').to_lowercase())
        })
        .collect()
}

fn tokenize(input: &str) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(char) = chars.next() {
        match char {
            '#' => {
                while let Some(next) = chars.next() {
                    if next == '\n' {
                        break;
                    }
                }
            }
            c if c.is_whitespace() => {}
            ':' => tokens.push(Token::Colon),
            ';' => tokens.push(Token::Semicolon),
            ',' => tokens.push(Token::Comma),
            '(' => tokens.push(Token::LeftParen),
            ')' => tokens.push(Token::RightParen),
            '{' => tokens.push(Token::LeftBrace),
            '}' => tokens.push(Token::RightBrace),
            '[' => tokens.push(Token::LeftBracket),
            ']' => tokens.push(Token::RightBracket),
            '"' => {
                let mut value = String::new();
                let mut escaped = false;
                while let Some(next) = chars.next() {
                    if escaped {
                        value.push(next);
                        escaped = false;
                        continue;
                    }
                    if next == '\\' {
                        escaped = true;
                        continue;
                    }
                    if next == '"' {
                        break;
                    }
                    value.push(next);
                }
                tokens.push(Token::String(value));
            }
            c if c.is_ascii_digit() => {
                let mut value = c.to_string();
                while let Some(next) = chars.peek() {
                    if next.is_ascii_digit() {
                        value.push(*next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(Token::Number(value.parse()?));
            }
            c if is_identifier_start(c) => {
                let mut value = c.to_string();
                while let Some(next) = chars.peek() {
                    if is_identifier_char(*next) {
                        value.push(*next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(Token::Identifier(value.to_lowercase()));
            }
            other => bail!("unsupported sieve token `{other}`"),
        }
    }

    Ok(tokens)
}

fn is_identifier_start(value: char) -> bool {
    value.is_ascii_alphabetic() || value == '_'
}

fn is_identifier_char(value: char) -> bool {
    value.is_ascii_alphanumeric() || matches!(value, '_' | '-')
}

struct Parser<'a> {
    tokens: &'a [Token],
    position: usize,
}

impl<'a> Parser<'a> {
    fn parse_script(&mut self) -> Result<Script> {
        let mut requirements = Vec::new();
        while self.consume_identifier("require") {
            requirements.extend(self.parse_string_list()?);
            self.expect_semicolon()?;
        }
        validate_requirements(&requirements)?;

        let mut statements = Vec::new();
        while !self.is_eof() {
            statements.push(self.parse_statement()?);
        }

        Ok(Script {
            requirements,
            statements,
        })
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        if self.consume_identifier("if") {
            return self.parse_if();
        }
        Ok(Statement::Action(self.parse_action()?))
    }

    fn parse_if(&mut self) -> Result<Statement> {
        let mut branches = Vec::new();
        branches.push((self.parse_test()?, self.parse_block()?));

        while self.consume_identifier("elsif") {
            branches.push((self.parse_test()?, self.parse_block()?));
        }

        let else_block = if self.consume_identifier("else") {
            Some(self.parse_block()?)
        } else {
            None
        };

        Ok(Statement::If {
            branches,
            else_block,
        })
    }

    fn parse_block(&mut self) -> Result<Vec<Statement>> {
        self.expect(Token::LeftBrace)?;
        let mut statements = Vec::new();
        while !self.consume(&Token::RightBrace) {
            if self.is_eof() {
                bail!("unterminated sieve block");
            }
            statements.push(self.parse_statement()?);
        }
        Ok(statements)
    }

    fn parse_test(&mut self) -> Result<Test> {
        if self.consume_identifier("allof") {
            return Ok(Test::AllOf(self.parse_test_list()?));
        }
        if self.consume_identifier("anyof") {
            return Ok(Test::AnyOf(self.parse_test_list()?));
        }
        if self.consume_identifier("not") {
            return Ok(Test::Not(Box::new(self.parse_test()?)));
        }
        if self.consume_identifier("true") {
            return Ok(Test::True);
        }
        if self.consume_identifier("false") {
            return Ok(Test::False);
        }
        if self.consume_identifier("header") {
            let match_type = self.parse_match_type()?;
            let fields = self.parse_string_list()?;
            let keys = self.parse_string_list()?;
            return Ok(Test::Header {
                match_type,
                fields,
                keys,
            });
        }
        if self.consume_identifier("address") {
            let match_type = self.parse_match_type()?;
            let fields = self.parse_string_list()?;
            let keys = self.parse_string_list()?;
            return Ok(Test::Address {
                match_type,
                fields,
                keys,
            });
        }
        if self.consume_identifier("envelope") {
            let match_type = self.parse_match_type()?;
            let parts = self.parse_string_list()?;
            let keys = self.parse_string_list()?;
            return Ok(Test::Envelope {
                match_type,
                parts,
                keys,
            });
        }
        Err(anyhow!("unsupported sieve test"))
    }

    fn parse_test_list(&mut self) -> Result<Vec<Test>> {
        self.expect(Token::LeftParen)?;
        let mut tests = Vec::new();
        loop {
            tests.push(self.parse_test()?);
            if self.consume(&Token::Comma) {
                continue;
            }
            self.expect(Token::RightParen)?;
            break;
        }
        Ok(tests)
    }

    fn parse_action(&mut self) -> Result<Action> {
        if self.consume_identifier("keep") {
            self.expect_semicolon()?;
            return Ok(Action::Keep);
        }
        if self.consume_identifier("discard") {
            self.expect_semicolon()?;
            return Ok(Action::Discard);
        }
        if self.consume_identifier("stop") {
            self.expect_semicolon()?;
            return Ok(Action::Stop);
        }
        if self.consume_identifier("fileinto") {
            let mailbox = self.expect_string()?;
            self.expect_semicolon()?;
            return Ok(Action::FileInto(mailbox));
        }
        if self.consume_identifier("redirect") {
            let target = self.expect_string()?;
            self.expect_semicolon()?;
            return Ok(Action::Redirect(target.to_lowercase()));
        }
        if self.consume_identifier("vacation") {
            let mut subject = None;
            let mut days = 7;
            while self.consume(&Token::Colon) {
                let tag = self.expect_identifier()?;
                match tag.as_str() {
                    "subject" => subject = Some(self.expect_string()?),
                    "days" => days = self.expect_number()?,
                    other => bail!("unsupported vacation tag `{other}`"),
                }
            }
            let reason = self.expect_string()?;
            self.expect_semicolon()?;
            return Ok(Action::Vacation {
                subject,
                days,
                reason,
            });
        }
        Err(anyhow!("unsupported sieve action"))
    }

    fn parse_match_type(&mut self) -> Result<MatchType> {
        self.expect(Token::Colon)?;
        match self.expect_identifier()?.as_str() {
            "is" => Ok(MatchType::Is),
            "contains" => Ok(MatchType::Contains),
            other => bail!("unsupported match type `{other}`"),
        }
    }

    fn parse_string_list(&mut self) -> Result<Vec<String>> {
        if self.consume(&Token::LeftBracket) {
            let mut values = Vec::new();
            loop {
                values.push(self.expect_string()?);
                if self.consume(&Token::Comma) {
                    continue;
                }
                self.expect(Token::RightBracket)?;
                break;
            }
            return Ok(values);
        }
        Ok(vec![self.expect_string()?])
    }

    fn expect_string(&mut self) -> Result<String> {
        match self.next() {
            Some(Token::String(value)) => Ok(value.clone()),
            other => Err(anyhow!("expected string, got {:?}", other)),
        }
    }

    fn expect_identifier(&mut self) -> Result<String> {
        match self.next() {
            Some(Token::Identifier(value)) => Ok(value.clone()),
            other => Err(anyhow!("expected identifier, got {:?}", other)),
        }
    }

    fn expect_number(&mut self) -> Result<u32> {
        match self.next() {
            Some(Token::Number(value)) => Ok(*value),
            other => Err(anyhow!("expected number, got {:?}", other)),
        }
    }

    fn expect_semicolon(&mut self) -> Result<()> {
        self.expect(Token::Semicolon)
    }

    fn expect(&mut self, expected: Token) -> Result<()> {
        if self.consume(&expected) {
            Ok(())
        } else {
            Err(anyhow!("expected token {:?}", expected))
        }
    }

    fn consume_identifier(&mut self, expected: &str) -> bool {
        match self.peek() {
            Some(Token::Identifier(value)) if value == expected => {
                self.position += 1;
                true
            }
            _ => false,
        }
    }

    fn consume(&mut self, expected: &Token) -> bool {
        match self.peek() {
            Some(value) if value == expected => {
                self.position += 1;
                true
            }
            _ => false,
        }
    }

    fn peek(&self) -> Option<&'a Token> {
        self.tokens.get(self.position)
    }

    fn next(&mut self) -> Option<&'a Token> {
        let token = self.tokens.get(self.position);
        if token.is_some() {
            self.position += 1;
        }
        token
    }

    fn is_eof(&self) -> bool {
        self.position >= self.tokens.len()
    }
}

fn validate_requirements(requirements: &[String]) -> Result<()> {
    for requirement in requirements {
        if !ALLOWED_REQUIREMENTS
            .iter()
            .any(|allowed| allowed == &requirement.as_str())
        {
            bail!("unsupported sieve requirement `{requirement}`");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> MessageContext {
        MessageContext {
            envelope_from: "sender@example.test".to_string(),
            envelope_to: "alice@example.test".to_string(),
            headers: BTreeMap::from([
                ("subject".to_string(), vec!["Quarterly report".to_string()]),
                (
                    "from".to_string(),
                    vec!["Sender <sender@example.test>".to_string()],
                ),
                (
                    "to".to_string(),
                    vec!["Alice <alice@example.test>".to_string()],
                ),
            ]),
        }
    }

    #[test]
    fn parses_requirements_and_if_blocks() {
        let script = parse_script(
            r#"
            require ["fileinto", "vacation"];
            if header :contains "subject" "report" {
                fileinto "Reports";
                stop;
            } elsif envelope :is "to" "alice@example.test" {
                vacation :subject "Out" :days 3 "Away";
            } else {
                keep;
            }
            "#,
        )
        .unwrap();

        assert_eq!(script.requirements, vec!["fileinto", "vacation"]);
        assert_eq!(script.statements.len(), 1);
    }

    #[test]
    fn rejects_unsupported_requirements() {
        let error = parse_script(r#"require "imapflags"; keep;"#).unwrap_err();
        assert!(error.to_string().contains("unsupported sieve requirement"));
    }

    #[test]
    fn evaluates_fileinto_and_stop() {
        let script = parse_script(
            r#"
            if header :contains "subject" "report" {
                fileinto "Reports";
                stop;
            }
            keep;
            "#,
        )
        .unwrap();

        let outcome = evaluate_script(&script, &context()).unwrap();

        assert!(!outcome.keep);
        assert_eq!(outcome.file_into.as_deref(), Some("Reports"));
        assert!(!outcome.discard);
    }

    #[test]
    fn evaluates_redirect_and_vacation_without_cancelling_keep() {
        let script = parse_script(
            r#"
            if address :is "from" "sender@example.test" {
                redirect "archive@example.test";
                vacation :subject "Auto reply" "Back tomorrow";
            }
            "#,
        )
        .unwrap();

        let outcome = evaluate_script(&script, &context()).unwrap();

        assert!(outcome.keep);
        assert_eq!(outcome.redirects, vec!["archive@example.test"]);
        assert_eq!(
            outcome.vacation,
            Some(VacationAction {
                subject: Some("Auto reply".to_string()),
                days: 7,
                reason: "Back tomorrow".to_string(),
            })
        );
    }

    #[test]
    fn discard_cancels_keep() {
        let script = parse_script(r#"if true { discard; }"#).unwrap();
        let outcome = evaluate_script(&script, &context()).unwrap();
        assert!(outcome.discard);
        assert!(!outcome.keep);
        assert!(outcome.file_into.is_none());
    }
}
