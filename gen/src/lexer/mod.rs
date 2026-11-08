use crate::ParseError;

#[derive(Debug, Clone, PartialEq)]
pub enum Kind {
    Word(String),
    Const(String),
    Symbol(String),
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Nil,
    Duration { amount: u64, unit: String },
    LeftBracket,
    RightBracket,
    OptionalRightBracket,
    LeftBrace,
    RightBrace,
    Comma,
    Colon,
    Newline,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: Kind,
    pub line: usize,
    pub column: usize,
}

const DURATION_UNITS: [&str; 10] = ["millisecond", "milliseconds", "second", "seconds", "minute", "minutes", "hour", "hours", "day", "days"];

struct Cursor {
    chars: Vec<char>,
    index: usize,
    line: usize,
    column: usize,
}

impl Cursor {
    fn peek(&self, offset: usize) -> Option<char> {
        self.chars.get(self.index + offset).copied()
    }

    fn advance(&mut self) -> Option<char> {
        let character = self.peek(0)?;
        self.index += 1;
        if character == '\n' {
            self.line += 1;
            self.column = 1;
        } else {
            self.column += 1;
        }
        Some(character)
    }

    fn take_while(&mut self, accepts: impl Fn(char) -> bool) -> String {
        let mut text = String::new();
        while let Some(character) = self.peek(0).filter(|character| accepts(*character)) {
            text.push(character);
            self.advance();
        }
        text
    }
}

fn is_name_character(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

fn reject_trailing_underscore(name: &str, line: usize, column: usize) -> Result<(), ParseError> {
    if name.trim_end_matches('?').ends_with('_') {
        return Err(ParseError::new(line, column, format!("name `{name}` ends with `_`: the generated code reserves that ending for its own names")));
    }
    Ok(())
}

pub fn lex(source: &str) -> Result<Vec<Token>, ParseError> {
    let mut cursor = Cursor { chars: source.chars().collect(), index: 0, line: 1, column: 1 };
    let mut tokens: Vec<Token> = Vec::new();
    while let Some(character) = cursor.peek(0) {
        let (line, column) = (cursor.line, cursor.column);
        let kind = match character {
            ' ' | '\t' | '\r' => {
                cursor.advance();
                continue;
            }
            '#' => {
                cursor.take_while(|character| character != '\n');
                continue;
            }
            '\n' => {
                cursor.advance();
                if matches!(tokens.last(), Some(Token { kind: Kind::Comma, .. })) {
                    continue;
                }
                Kind::Newline
            }
            '[' | ']' | '{' | '}' | ',' => {
                cursor.advance();
                match character {
                    '[' => Kind::LeftBracket,
                    ']' if cursor.peek(0) == Some('?') => {
                        cursor.advance();
                        Kind::OptionalRightBracket
                    }
                    ']' => Kind::RightBracket,
                    '{' => Kind::LeftBrace,
                    '}' => Kind::RightBrace,
                    _ => Kind::Comma,
                }
            }
            ':' if cursor.peek(1).is_some_and(is_name_character) => {
                cursor.advance();
                let mut name = cursor.take_while(is_name_character);
                if cursor.peek(0) == Some('?') {
                    cursor.advance();
                    name.push('?');
                }
                reject_trailing_underscore(&name, line, column)?;
                Kind::Symbol(name)
            }
            ':' => {
                cursor.advance();
                Kind::Colon
            }
            '"' => lex_string(&mut cursor, line, column)?,
            '-' if cursor.peek(1).is_some_and(|next| next.is_ascii_digit()) => lex_number(&mut cursor, line, column)?,
            digit if digit.is_ascii_digit() => lex_number(&mut cursor, line, column)?,
            letter if letter.is_alphabetic() || letter == '_' => {
                let text = cursor.take_while(is_name_character);
                reject_trailing_underscore(&text, line, column)?;
                match text.as_str() {
                    "true" => Kind::Bool(true),
                    "false" => Kind::Bool(false),
                    "nil" => Kind::Nil,
                    _ if letter.is_uppercase() => Kind::Const(text),
                    _ => Kind::Word(text),
                }
            }
            other => return Err(ParseError::new(line, column, format!("unexpected character `{other}`"))),
        };
        tokens.push(Token { kind, line, column });
    }
    Ok(tokens)
}

fn lex_string(cursor: &mut Cursor, line: usize, column: usize) -> Result<Kind, ParseError> {
    cursor.advance();
    let text = cursor.take_while(|character| character != '"' && character != '\n');
    match cursor.advance() {
        Some('"') => Ok(Kind::Str(text)),
        _ => Err(ParseError::new(line, column, "unterminated string")),
    }
}

fn lex_number(cursor: &mut Cursor, line: usize, column: usize) -> Result<Kind, ParseError> {
    let negative = cursor.peek(0) == Some('-');
    if negative {
        cursor.advance();
    }
    let digits = cursor.take_while(|character| character.is_ascii_digit());
    let fraction = cursor.peek(0) == Some('.') && cursor.peek(1).is_some_and(|next| next.is_ascii_digit());
    if fraction {
        cursor.advance();
        let decimals = cursor.take_while(|character| character.is_ascii_digit());
        let value: f64 = format!("{digits}.{decimals}").parse().expect("digits form a float");
        return Ok(Kind::Float(if negative { -value } else { value }));
    }
    let unit_follows = cursor.peek(0) == Some('.') && cursor.peek(1).is_some_and(char::is_alphabetic);
    if unit_follows && !negative {
        cursor.advance();
        let unit = cursor.take_while(is_name_character);
        if !DURATION_UNITS.contains(&unit.as_str()) {
            return Err(ParseError::new(line, column, format!("unknown duration unit `{unit}`")));
        }
        let amount = digits.parse().map_err(|_| ParseError::new(line, column, "number is too large"))?;
        return Ok(Kind::Duration { amount, unit });
    }
    let value: i64 = digits.parse().map_err(|_| ParseError::new(line, column, "number is too large"))?;
    Ok(Kind::Int(if negative { -value } else { value }))
}

#[cfg(test)]
mod tests;
