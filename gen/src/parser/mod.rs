use crate::lexer::{Kind, Token};
use crate::ParseError;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Symbol(String),
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Nil,
    Const(String),
    Duration { amount: u64, unit: String },
    Array(Vec<Node<Value>>),
    Hash(Vec<Pair>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Node<T> {
    pub item: T,
    pub line: usize,
    pub column: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pair {
    pub key: String,
    pub line: usize,
    pub column: usize,
    pub value: Node<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Call {
    pub word: String,
    pub line: usize,
    pub column: usize,
    pub args: Vec<Node<Value>>,
    pub options: Vec<Pair>,
    pub block: Option<Vec<Call>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelNode {
    pub name: String,
    pub line: usize,
    pub column: usize,
    pub file: usize,
    pub options: Vec<Pair>,
    pub body: Vec<Call>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OperationNode {
    pub name: String,
    pub line: usize,
    pub column: usize,
    pub file: usize,
    pub body: Vec<Call>,
}

pub struct Parsed {
    pub models: Vec<ModelNode>,
    pub operations: Vec<OperationNode>,
}

struct Reader<'a> {
    tokens: &'a [Token],
    index: usize,
}

impl<'a> Reader<'a> {
    fn peek(&self) -> Option<&'a Token> {
        self.tokens.get(self.index)
    }

    fn peek_kind(&self, offset: usize) -> Option<&'a Kind> {
        self.tokens.get(self.index + offset).map(|token| &token.kind)
    }

    fn next(&mut self) -> Option<&'a Token> {
        let token = self.peek();
        self.index += 1;
        token
    }

    fn at_statement_end(&self) -> bool {
        matches!(self.peek_kind(0), None | Some(Kind::Newline))
    }

    fn at_block_start(&self) -> bool {
        matches!(self.peek_kind(0), Some(Kind::Word(word)) if word == "do")
    }

    fn skip_newlines(&mut self) {
        while matches!(self.peek_kind(0), Some(Kind::Newline)) {
            self.index += 1;
        }
    }

    fn last_position(&self) -> (usize, usize) {
        self.tokens.last().map_or((1, 1), |token| (token.line, token.column))
    }

    fn unexpected(&self) -> ParseError {
        match self.peek() {
            Some(token) => ParseError::new(token.line, token.column, format!("unexpected {}", describe(&token.kind))),
            None => {
                let (line, column) = self.last_position();
                ParseError::new(line, column, "unexpected end of file")
            }
        }
    }
}

fn describe(kind: &Kind) -> String {
    match kind {
        Kind::Word(text) | Kind::Const(text) => format!("`{text}`"),
        Kind::Symbol(text) => format!("`:{text}`"),
        Kind::Str(text) => format!("\"{text}\""),
        Kind::Int(number) => format!("`{number}`"),
        Kind::Float(number) => format!("`{number}`"),
        Kind::Bool(flag) => format!("`{flag}`"),
        Kind::Nil => "`nil`".to_string(),
        Kind::Duration { amount, unit } => format!("`{amount}.{unit}`"),
        Kind::LeftBracket => "`[`".to_string(),
        Kind::RightBracket => "`]`".to_string(),
        Kind::LeftBrace => "`{`".to_string(),
        Kind::RightBrace => "`}`".to_string(),
        Kind::Comma => "`,`".to_string(),
        Kind::Colon => "`:`".to_string(),
        Kind::Newline => "end of line".to_string(),
    }
}

pub fn parse(tokens: &[Token], file: usize) -> Result<Parsed, ParseError> {
    let mut reader = Reader { tokens, index: 0 };
    let mut parsed = Parsed { models: Vec::new(), operations: Vec::new() };
    loop {
        reader.skip_newlines();
        if reader.peek().is_none() {
            return Ok(parsed);
        }
        let call = parse_call(&mut reader)?;
        if call.word == "operation" {
            parsed.operations.push(operation_from(call, file)?);
        } else {
            parsed.models.push(model_from(call, file)?);
        }
    }
}

fn operation_from(call: Call, file: usize) -> Result<OperationNode, ParseError> {
    let at = |message: String| ParseError::new(call.line, call.column, message);
    let name = match call.args.as_slice() {
        [Node { item: Value::Symbol(name), .. }] => name.clone(),
        _ => return Err(at("operation needs a name like `operation :qr_code_create do`".to_string())),
    };
    let Some(body) = call.block else {
        return Err(at(format!("operation {name} needs a `do ... end` block")));
    };
    Ok(OperationNode { name, line: call.line, column: call.column, file, body })
}

fn model_from(call: Call, file: usize) -> Result<ModelNode, ParseError> {
    let at = |message: String| ParseError::new(call.line, call.column, message);
    if call.word != "model" {
        return Err(at(format!("expected `model` or `operation`, found `{}`", call.word)));
    }
    let name = match call.args.as_slice() {
        [Node { item: Value::Const(name), .. }] => name.clone(),
        _ => return Err(at("model needs a constant name, for example `model User do`".to_string())),
    };
    let Some(body) = call.block else {
        return Err(at(format!("model {name} needs a `do ... end` block")));
    };
    Ok(ModelNode { name, line: call.line, column: call.column, file, options: call.options, body })
}

fn parse_call(reader: &mut Reader) -> Result<Call, ParseError> {
    let Some(Token { kind: Kind::Word(word), line, column }) = reader.next() else {
        reader.index -= 1;
        return Err(reader.unexpected());
    };
    let mut call = Call { word: word.clone(), line: *line, column: *column, args: Vec::new(), options: Vec::new(), block: None };
    while !reader.at_statement_end() && !reader.at_block_start() {
        parse_item(reader, &mut call.args, &mut call.options)?;
        if matches!(reader.peek_kind(0), Some(Kind::Comma)) {
            reader.next();
            if reader.at_statement_end() {
                return Err(reader.unexpected());
            }
        } else {
            break;
        }
    }
    if reader.at_block_start() {
        reader.next();
        call.block = Some(parse_block(reader, &call)?);
    }
    if !reader.at_statement_end() {
        return Err(reader.unexpected());
    }
    Ok(call)
}

fn parse_block(reader: &mut Reader, opener: &Call) -> Result<Vec<Call>, ParseError> {
    if !reader.at_statement_end() {
        return Err(reader.unexpected());
    }
    let mut calls = Vec::new();
    loop {
        reader.skip_newlines();
        match reader.peek_kind(0) {
            None => {
                let message = format!("`{}` block is not closed with `end`", opener.word);
                return Err(ParseError::new(opener.line, opener.column, message));
            }
            Some(Kind::Word(word)) if word == "end" => {
                reader.next();
                return Ok(calls);
            }
            Some(_) => calls.push(parse_call(reader)?),
        }
    }
}

fn parse_item(reader: &mut Reader, args: &mut Vec<Node<Value>>, options: &mut Vec<Pair>) -> Result<(), ParseError> {
    if let (Some(Kind::Word(_)), Some(Kind::Colon)) = (reader.peek_kind(0), reader.peek_kind(1)) {
        options.push(parse_pair(reader)?);
    } else {
        args.push(parse_value(reader)?);
    }
    Ok(())
}

fn parse_pair(reader: &mut Reader) -> Result<Pair, ParseError> {
    let Some(Token { kind: Kind::Word(key), line, column }) = reader.next() else {
        reader.index -= 1;
        return Err(reader.unexpected());
    };
    reader.next();
    let value = parse_value(reader)?;
    Ok(Pair { key: key.clone(), line: *line, column: *column, value })
}

fn parse_value(reader: &mut Reader) -> Result<Node<Value>, ParseError> {
    let Some(token) = reader.peek() else {
        return Err(reader.unexpected());
    };
    let item = match &token.kind {
        Kind::Symbol(text) => Value::Symbol(text.clone()),
        Kind::Str(text) => Value::Str(text.clone()),
        Kind::Int(number) => Value::Int(*number),
        Kind::Float(number) => Value::Float(*number),
        Kind::Bool(flag) => Value::Bool(*flag),
        Kind::Nil => Value::Nil,
        Kind::Const(text) => Value::Const(text.clone()),
        Kind::Duration { amount, unit } => Value::Duration { amount: *amount, unit: unit.clone() },
        Kind::LeftBracket => return parse_array(reader),
        Kind::LeftBrace => return parse_hash(reader),
        _ => return Err(reader.unexpected()),
    };
    reader.next();
    Ok(Node { item, line: token.line, column: token.column })
}

fn parse_array(reader: &mut Reader) -> Result<Node<Value>, ParseError> {
    let open = reader.next().expect("bracket checked by caller");
    let mut items = Vec::new();
    loop {
        reader.skip_newlines();
        if matches!(reader.peek_kind(0), Some(Kind::RightBracket)) {
            reader.next();
            return Ok(Node { item: Value::Array(items), line: open.line, column: open.column });
        }
        items.push(parse_value(reader)?);
        reader.skip_newlines();
        match reader.peek_kind(0) {
            Some(Kind::Comma) => {
                reader.next();
            }
            Some(Kind::RightBracket) => {}
            _ => return Err(reader.unexpected()),
        }
    }
}

fn parse_hash(reader: &mut Reader) -> Result<Node<Value>, ParseError> {
    let open = reader.next().expect("brace checked by caller");
    let mut pairs = Vec::new();
    loop {
        reader.skip_newlines();
        if matches!(reader.peek_kind(0), Some(Kind::RightBrace)) {
            reader.next();
            return Ok(Node { item: Value::Hash(pairs), line: open.line, column: open.column });
        }
        if !matches!((reader.peek_kind(0), reader.peek_kind(1)), (Some(Kind::Word(_)), Some(Kind::Colon))) {
            return Err(reader.unexpected());
        }
        pairs.push(parse_pair(reader)?);
        reader.skip_newlines();
        match reader.peek_kind(0) {
            Some(Kind::Comma) => {
                reader.next();
            }
            Some(Kind::RightBrace) => {}
            _ => return Err(reader.unexpected()),
        }
    }
}

#[cfg(test)]
mod parse_tests;
