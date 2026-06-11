//! Minimal filter expression engine.
//!
//! Replaces the former Rhai-based engine with a direct parser/evaluator for
//! the deliberately tiny filter grammar: `SOURCE`/`TARGET` property reads
//! (`size`, `last_modified`), numeric literals, arithmetic (`+ - * / %`),
//! comparisons (`> >= < <= == !=`), boolean logic (`&& || !`), and
//! parentheses. Evaluation is a direct AST walk with no allocation, so
//! per-object cost is a few branches instead of an interpreter dispatch.

use crate::core::ObjectProps;

const MAX_EXPR_DEPTH: usize = 24;

// ── AST ────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Prop {
    Size,
    LastModified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Var {
    Source,
    Target,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnOp {
    Not,
    Neg,
    Plus,
}

#[derive(Debug, Clone)]
enum Node {
    Bool(bool),
    Int(i64),
    Float(f64),
    Prop(Var, Prop),
    Unary(UnOp, Box<Node>),
    Binary(BinOp, Box<Node>, Box<Node>),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
}

/// A compiled filter expression. Evaluation never allocates.
#[derive(Debug, Clone)]
pub struct FilterExpr {
    root: Node,
    uses_target: bool,
}

impl FilterExpr {
    /// Parse and validate an expression. `allow_target` rejects `TARGET`
    /// references at compile time (list mode has no target object).
    pub fn compile(expr: &str, allow_target: bool) -> Result<Self, String> {
        let root = Parser::new(expr).parse()?;
        let uses_target = uses_target(&root);
        if uses_target && !allow_target {
            return Err("variable \"TARGET\" is only available in diff mode".to_string());
        }
        let compiled = Self { root, uses_target };

        // Evaluate against default props to catch type errors (e.g. a
        // numeric-valued expression, or `!` applied to a number) up front.
        let probe = ObjectProps::default();
        let target_probe = if uses_target { Some(&probe) } else { None };
        match eval(&compiled.root, &probe, target_probe) {
            Some(Value::Bool(_)) => Ok(compiled),
            Some(_) => Err("filter expression must evaluate to a boolean".to_string()),
            None => Err("filter expression failed to evaluate".to_string()),
        }
    }

    /// Evaluate for an object pair. Returns `None` on runtime errors
    /// (e.g. division by zero, or `TARGET` referenced without a target).
    pub fn evaluate(&self, source: &ObjectProps, target: Option<&ObjectProps>) -> Option<bool> {
        if self.uses_target && target.is_none() {
            return None;
        }
        match eval(&self.root, source, target)? {
            Value::Bool(b) => Some(b),
            _ => None,
        }
    }
}

fn uses_target(node: &Node) -> bool {
    match node {
        Node::Prop(Var::Target, _) => true,
        Node::Prop(Var::Source, _) | Node::Bool(_) | Node::Int(_) | Node::Float(_) => false,
        Node::Unary(_, inner) => uses_target(inner),
        Node::Binary(_, lhs, rhs) => uses_target(lhs) || uses_target(rhs),
    }
}

// ── Evaluation ─────────────────────────────────────────────

fn eval(node: &Node, source: &ObjectProps, target: Option<&ObjectProps>) -> Option<Value> {
    match node {
        Node::Bool(b) => Some(Value::Bool(*b)),
        Node::Int(i) => Some(Value::Int(*i)),
        Node::Float(f) => Some(Value::Float(*f)),
        Node::Prop(var, prop) => {
            let props = match var {
                Var::Source => source,
                Var::Target => target?,
            };
            let raw = match prop {
                Prop::Size => props.size,
                Prop::LastModified => props.last_modified,
            };
            Some(Value::Int(i64::try_from(raw).ok()?))
        }
        Node::Unary(op, inner) => {
            let v = eval(inner, source, target)?;
            match (op, v) {
                (UnOp::Not, Value::Bool(b)) => Some(Value::Bool(!b)),
                (UnOp::Neg, Value::Int(i)) => Some(Value::Int(i.checked_neg()?)),
                (UnOp::Neg, Value::Float(f)) => Some(Value::Float(-f)),
                (UnOp::Plus, v @ (Value::Int(_) | Value::Float(_))) => Some(v),
                _ => None,
            }
        }
        Node::Binary(op, lhs, rhs) => {
            // Short-circuit boolean operators.
            if matches!(op, BinOp::And | BinOp::Or) {
                let Value::Bool(l) = eval(lhs, source, target)? else {
                    return None;
                };
                if (*op == BinOp::And && !l) || (*op == BinOp::Or && l) {
                    return Some(Value::Bool(l));
                }
                let Value::Bool(r) = eval(rhs, source, target)? else {
                    return None;
                };
                return Some(Value::Bool(r));
            }

            let l = eval(lhs, source, target)?;
            let r = eval(rhs, source, target)?;
            eval_binary(*op, l, r)
        }
    }
}

fn eval_binary(op: BinOp, l: Value, r: Value) -> Option<Value> {
    use Value::*;

    // Equality across mismatched value kinds is `false`, not an error
    // (numeric int/float pairs still compare by value).
    if matches!(op, BinOp::Eq | BinOp::Ne) {
        let equal = match (l, r) {
            (Bool(a), Bool(b)) => a == b,
            (Int(a), Int(b)) => a == b,
            (Float(a), Float(b)) => a == b,
            (Int(a), Float(b)) | (Float(b), Int(a)) => (a as f64) == b,
            _ => false,
        };
        return Some(Bool(if op == BinOp::Eq { equal } else { !equal }));
    }

    match (l, r) {
        (Int(a), Int(b)) => match op {
            BinOp::Add => a.checked_add(b).map(Int),
            BinOp::Sub => a.checked_sub(b).map(Int),
            BinOp::Mul => a.checked_mul(b).map(Int),
            BinOp::Div => a.checked_div(b).map(Int),
            BinOp::Rem => a.checked_rem(b).map(Int),
            BinOp::Lt => Some(Bool(a < b)),
            BinOp::Le => Some(Bool(a <= b)),
            BinOp::Gt => Some(Bool(a > b)),
            BinOp::Ge => Some(Bool(a >= b)),
            _ => None,
        },
        (Int(_) | Float(_), Int(_) | Float(_)) => {
            let a = match l {
                Int(i) => i as f64,
                Float(f) => f,
                _ => unreachable!(),
            };
            let b = match r {
                Int(i) => i as f64,
                Float(f) => f,
                _ => unreachable!(),
            };
            match op {
                BinOp::Add => Some(Float(a + b)),
                BinOp::Sub => Some(Float(a - b)),
                BinOp::Mul => Some(Float(a * b)),
                BinOp::Div => Some(Float(a / b)),
                BinOp::Rem => Some(Float(a % b)),
                BinOp::Lt => Some(Bool(a < b)),
                BinOp::Le => Some(Bool(a <= b)),
                BinOp::Gt => Some(Bool(a > b)),
                BinOp::Ge => Some(Bool(a >= b)),
                _ => None,
            }
        }
        _ => None,
    }
}

// ── Parser ─────────────────────────────────────────────────

struct Parser<'a> {
    input: &'a [u8],
    pos: usize,
    depth: usize,
}

impl<'a> Parser<'a> {
    fn new(expr: &'a str) -> Self {
        Self {
            input: expr.as_bytes(),
            pos: 0,
            depth: 0,
        }
    }

    fn parse(mut self) -> Result<Node, String> {
        let node = self.parse_or()?;
        self.skip_ws();
        if self.pos < self.input.len() {
            return Err(format!(
                "unexpected trailing input at byte {} in filter expression",
                self.pos
            ));
        }
        Ok(node)
    }

    fn enter(&mut self) -> Result<(), String> {
        self.depth += 1;
        if self.depth > MAX_EXPR_DEPTH {
            return Err(format!(
                "filter expression nesting exceeds depth limit {}",
                MAX_EXPR_DEPTH
            ));
        }
        Ok(())
    }

    fn leave(&mut self) {
        self.depth -= 1;
    }

    fn parse_or(&mut self) -> Result<Node, String> {
        self.enter()?;
        let mut node = self.parse_and()?;
        while self.eat_op("||") {
            let rhs = self.parse_and()?;
            node = Node::Binary(BinOp::Or, Box::new(node), Box::new(rhs));
        }
        self.leave();
        Ok(node)
    }

    fn parse_and(&mut self) -> Result<Node, String> {
        self.enter()?;
        let mut node = self.parse_cmp()?;
        while self.eat_op("&&") {
            let rhs = self.parse_cmp()?;
            node = Node::Binary(BinOp::And, Box::new(node), Box::new(rhs));
        }
        self.leave();
        Ok(node)
    }

    fn parse_cmp(&mut self) -> Result<Node, String> {
        self.enter()?;
        let mut node = self.parse_add()?;
        loop {
            let op = if self.eat_op(">=") {
                BinOp::Ge
            } else if self.eat_op("<=") {
                BinOp::Le
            } else if self.eat_op("==") {
                BinOp::Eq
            } else if self.eat_op("!=") {
                BinOp::Ne
            } else if self.eat_op(">") {
                BinOp::Gt
            } else if self.eat_op("<") {
                BinOp::Lt
            } else {
                break;
            };
            let rhs = self.parse_add()?;
            node = Node::Binary(op, Box::new(node), Box::new(rhs));
        }
        self.leave();
        Ok(node)
    }

    fn parse_add(&mut self) -> Result<Node, String> {
        self.enter()?;
        let mut node = self.parse_mul()?;
        loop {
            let op = if self.eat_op("+") {
                BinOp::Add
            } else if self.eat_op("-") {
                BinOp::Sub
            } else {
                break;
            };
            let rhs = self.parse_mul()?;
            node = Node::Binary(op, Box::new(node), Box::new(rhs));
        }
        self.leave();
        Ok(node)
    }

    fn parse_mul(&mut self) -> Result<Node, String> {
        self.enter()?;
        let mut node = self.parse_unary()?;
        loop {
            let op = if self.eat_op("*") {
                BinOp::Mul
            } else if self.eat_op("/") {
                BinOp::Div
            } else if self.eat_op("%") {
                BinOp::Rem
            } else {
                break;
            };
            let rhs = self.parse_unary()?;
            node = Node::Binary(op, Box::new(node), Box::new(rhs));
        }
        self.leave();
        Ok(node)
    }

    fn parse_unary(&mut self) -> Result<Node, String> {
        self.enter()?;
        self.skip_ws();
        let node = if self.eat_op("!") {
            // Reject `!=` mis-parse: eat_op("!") already skipped one byte;
            // a following `=` would have matched eat_op("!=") in parse_cmp,
            // so reaching here with `!` is genuine negation.
            Node::Unary(UnOp::Not, Box::new(self.parse_unary()?))
        } else if self.eat_op("-") {
            Node::Unary(UnOp::Neg, Box::new(self.parse_unary()?))
        } else if self.eat_op("+") {
            Node::Unary(UnOp::Plus, Box::new(self.parse_unary()?))
        } else {
            self.parse_primary()?
        };
        self.leave();
        Ok(node)
    }

    fn parse_primary(&mut self) -> Result<Node, String> {
        self.enter()?;
        self.skip_ws();
        let node = match self.input.get(self.pos) {
            Some(b'(') => {
                self.pos += 1;
                let inner = self.parse_or()?;
                self.skip_ws();
                if !self.eat_byte(b')') {
                    return Err("missing closing parenthesis in filter expression".to_string());
                }
                inner
            }
            Some(c) if c.is_ascii_digit() => self.parse_number()?,
            Some(c) if c.is_ascii_alphabetic() || *c == b'_' => self.parse_identifier()?,
            Some(b'"') | Some(b'\'') => {
                return Err(
                    "string and character literals are not supported in filters".to_string()
                );
            }
            Some(c) => {
                return Err(format!(
                    "unexpected character '{}' in filter expression",
                    *c as char
                ));
            }
            None => return Err("unexpected end of filter expression".to_string()),
        };
        self.leave();
        Ok(node)
    }

    fn parse_number(&mut self) -> Result<Node, String> {
        let start = self.pos;
        let mut is_float = false;
        while let Some(&c) = self.input.get(self.pos) {
            match c {
                b'0'..=b'9' | b'_' => self.pos += 1,
                b'.' => {
                    // A dot only continues the number if followed by a digit;
                    // otherwise it would be a (rejected) method/property access.
                    if self
                        .input
                        .get(self.pos + 1)
                        .is_some_and(|d| d.is_ascii_digit())
                    {
                        is_float = true;
                        self.pos += 1;
                    } else {
                        break;
                    }
                }
                b'e' | b'E' => {
                    let mut next = self.pos + 1;
                    if matches!(self.input.get(next), Some(b'+') | Some(b'-')) {
                        next += 1;
                    }
                    if self.input.get(next).is_some_and(|d| d.is_ascii_digit()) {
                        is_float = true;
                        self.pos = next;
                    } else {
                        break;
                    }
                }
                _ => break,
            }
        }
        let raw: String = std::str::from_utf8(&self.input[start..self.pos])
            .map_err(|_| "invalid number in filter expression".to_string())?
            .chars()
            .filter(|c| *c != '_')
            .collect();
        if is_float {
            raw.parse::<f64>()
                .map(Node::Float)
                .map_err(|_| format!("invalid float literal \"{}\" in filter expression", raw))
        } else {
            raw.parse::<i64>()
                .map(Node::Int)
                .map_err(|_| format!("invalid integer literal \"{}\" in filter expression", raw))
        }
    }

    fn parse_identifier(&mut self) -> Result<Node, String> {
        let name = self.read_word();
        match name {
            "true" => return Ok(Node::Bool(true)),
            "false" => return Ok(Node::Bool(false)),
            "SOURCE" | "TARGET" => {}
            _ => {
                self.skip_ws();
                if self.input.get(self.pos) == Some(&b'(') {
                    return Err(format!("function call \"{}\" not allowed in filter", name));
                }
                return Err(format!("variable \"{}\" not allowed in filter", name));
            }
        }
        let var = if name == "SOURCE" {
            Var::Source
        } else {
            Var::Target
        };

        self.skip_ws();
        if !self.eat_byte(b'.') {
            return Err(format!(
                "variable \"{}\" must be used as a property access (e.g. {}.size)",
                name, name
            ));
        }
        self.skip_ws();
        let prop_start = self.pos;
        let prop = self.read_word();
        match prop {
            "size" => Ok(Node::Prop(var, Prop::Size)),
            "last_modified" => Ok(Node::Prop(var, Prop::LastModified)),
            "" => Err(format!(
                "missing property name after \"{}.\" at byte {}",
                name, prop_start
            )),
            other => {
                self.skip_ws();
                if self.input.get(self.pos) == Some(&b'(') {
                    return Err(format!("method call \"{}\" not allowed in filter", other));
                }
                Err(format!(
                    "object property \"{}\" not allowed in filter",
                    other
                ))
            }
        }
    }

    fn read_word(&mut self) -> &'a str {
        let start = self.pos;
        while let Some(&c) = self.input.get(self.pos) {
            if c.is_ascii_alphanumeric() || c == b'_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        // Identifiers are ASCII by construction.
        std::str::from_utf8(&self.input[start..self.pos]).unwrap_or("")
    }

    fn skip_ws(&mut self) {
        while self
            .input
            .get(self.pos)
            .is_some_and(|c| c.is_ascii_whitespace())
        {
            self.pos += 1;
        }
    }

    fn eat_byte(&mut self, b: u8) -> bool {
        if self.input.get(self.pos) == Some(&b) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    /// Consume `op` if present at the cursor (after whitespace). Single-char
    /// operators that prefix a longer operator (`!` vs `!=`, `>` vs `>=`)
    /// must be tried after their two-char forms by callers; `eat_op` itself
    /// refuses to match `!`, `>` or `<` when followed by `=`, and `+`/`-`
    /// are never prefixes of longer operators in this grammar.
    fn eat_op(&mut self, op: &str) -> bool {
        self.skip_ws();
        let bytes = op.as_bytes();
        if self.input.len() - self.pos < bytes.len() {
            return false;
        }
        if &self.input[self.pos..self.pos + bytes.len()] != bytes {
            return false;
        }
        if bytes.len() == 1 && matches!(bytes[0], b'!' | b'>' | b'<' | b'=') {
            if self.input.get(self.pos + 1) == Some(&b'=') {
                return false;
            }
        }
        // Reject a lone `&` or `|` (only `&&`/`||` exist in this grammar).
        self.pos += bytes.len();
        true
    }
}

// ── Tests ──────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn props(size: u64, last_modified: u64) -> ObjectProps {
        let mut p = ObjectProps::default();
        p.size = size;
        p.last_modified = last_modified;
        p
    }

    fn eval_list(expr: &str, source: &ObjectProps) -> Option<bool> {
        FilterExpr::compile(expr, false)
            .unwrap()
            .evaluate(source, None)
    }

    #[test]
    fn test_simple_comparisons() {
        let p = props(2048, 1715700001);
        assert_eq!(eval_list("SOURCE.size > 1000", &p), Some(true));
        assert_eq!(eval_list("SOURCE.size < 1000", &p), Some(false));
        assert_eq!(eval_list("SOURCE.size == 2048", &p), Some(true));
        assert_eq!(eval_list("SOURCE.size != 2048", &p), Some(false));
        assert_eq!(eval_list("SOURCE.last_modified >= 1715700000", &p), Some(true));
        assert_eq!(eval_list("SOURCE.last_modified <= 1715700000", &p), Some(false));
    }

    #[test]
    fn test_boolean_logic_and_grouping() {
        let p = props(2048, 100);
        assert_eq!(
            eval_list("SOURCE.size > 1000 && SOURCE.last_modified >= 100", &p),
            Some(true)
        );
        assert_eq!(
            eval_list("SOURCE.size > 9999 || SOURCE.last_modified == 100", &p),
            Some(true)
        );
        assert_eq!(eval_list("!(SOURCE.size > 1000)", &p), Some(false));
        assert_eq!(
            eval_list("(SOURCE.size > 1000 || false) && true", &p),
            Some(true)
        );
    }

    #[test]
    fn test_arithmetic() {
        let p = props(1024, 0);
        assert_eq!(eval_list("SOURCE.size * 2 == 2048", &p), Some(true));
        assert_eq!(eval_list("SOURCE.size + 1 > 1024", &p), Some(true));
        assert_eq!(eval_list("SOURCE.size - 24 == 1000", &p), Some(true));
        assert_eq!(eval_list("SOURCE.size / 2 == 512", &p), Some(true));
        assert_eq!(eval_list("SOURCE.size % 1000 == 24", &p), Some(true));
        assert_eq!(eval_list("-SOURCE.size < 0", &p), Some(true));
        assert_eq!(eval_list("SOURCE.size > 1.5e3", &p), Some(false));
        assert_eq!(eval_list("SOURCE.size > 0.5", &p), Some(true));
    }

    #[test]
    fn test_runtime_errors_return_none() {
        // Compile-time probe uses size=0, so these pass compilation but can
        // fail at runtime for other objects.
        let f = FilterExpr::compile("1000 / (1 - SOURCE.size) > 0", false).unwrap();
        assert_eq!(f.evaluate(&props(0, 0), None), Some(true));
        assert_eq!(f.evaluate(&props(1, 0), None), None); // division by zero

        let f = FilterExpr::compile("SOURCE.size + 1 > 0", false).unwrap();
        assert_eq!(f.evaluate(&props(u64::MAX, 0), None), None); // i64 overflow
    }

    #[test]
    fn test_target_in_diff_mode() {
        let f = FilterExpr::compile("SOURCE.size > TARGET.size", true).unwrap();
        assert_eq!(f.evaluate(&props(2000, 0), Some(&props(1000, 0))), Some(true));
        assert_eq!(f.evaluate(&props(500, 0), Some(&props(1000, 0))), Some(false));
        // No target at runtime → error, not a panic.
        assert_eq!(f.evaluate(&props(2000, 0), None), None);
    }

    #[test]
    fn test_target_rejected_in_list_mode() {
        let err = FilterExpr::compile("SOURCE.size > TARGET.size", false).unwrap_err();
        assert!(err.contains("TARGET"), "{}", err);
    }

    #[test]
    fn test_rejects_unknown_identifiers() {
        assert!(FilterExpr::compile("OTHER > 5", false)
            .unwrap_err()
            .contains("not allowed"));
        assert!(FilterExpr::compile("SOURCE.etag == 1", false)
            .unwrap_err()
            .contains("not allowed"));
        assert!(FilterExpr::compile("max(SOURCE.size, 1) > 0", false)
            .unwrap_err()
            .contains("not allowed"));
        assert!(FilterExpr::compile("SOURCE.trim() == 1", true)
            .unwrap_err()
            .contains("not allowed"));
    }

    #[test]
    fn test_rejects_non_boolean_expressions() {
        assert!(FilterExpr::compile("SOURCE.size + 1", false).is_err());
        assert!(FilterExpr::compile("42", false).is_err());
        assert!(FilterExpr::compile("!SOURCE.size", false).is_err());
        assert!(FilterExpr::compile("true && 1", false).is_err());
    }

    #[test]
    fn test_rejects_malformed_input() {
        assert!(FilterExpr::compile("", false).is_err());
        assert!(FilterExpr::compile("SOURCE.size >", false).is_err());
        assert!(FilterExpr::compile("(SOURCE.size > 1", false).is_err());
        assert!(FilterExpr::compile("SOURCE.size > 1)", false).is_err());
        assert!(FilterExpr::compile("SOURCE.size > 1; true", false).is_err());
        assert!(FilterExpr::compile("SOURCE size > 1", false).is_err());
        assert!(FilterExpr::compile("SOURCE.size & 1", false).is_err());
        assert!(FilterExpr::compile("SOURCE.size = 1", false).is_err());
    }

    #[test]
    fn test_depth_limit() {
        let expr = format!("{}true{}", "(".repeat(60), ")".repeat(60));
        let err = FilterExpr::compile(&expr, false).unwrap_err();
        assert!(err.contains("depth"), "{}", err);
    }

    #[test]
    fn test_mismatched_equality_kinds_compare_false() {
        let p = props(1, 0);
        assert_eq!(eval_list("(SOURCE.size == 1) == true", &p), Some(true));
        assert_eq!(eval_list("true != false", &p), Some(true));
        assert_eq!(eval_list("SOURCE.size == 1.0", &p), Some(true));
    }
}
