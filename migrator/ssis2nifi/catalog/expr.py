"""Translate SSIS expression-language text into Calcite SQL (what NiFi's
QueryRecord actually executes).

SCOPE, DELIBERATELY BOUNDED
----------------------------
This is not a general SSIS expression compiler. It supports exactly the
subset a ConditionalSplit predicate or a DerivedColumn expression realistically
needs for row-level filtering and simple computed columns: comparisons,
boolean logic, arithmetic, string concatenation, a handful of functions, and
casts. Anything outside that subset raises ExprUnsupported rather than
emitting SQL that looks plausible and is wrong -- the same "refuse rather than
guess" rule the rest of this converter follows for whole components (see
catalog/support.py).

GRAMMAR (precedence, low to high)
----------------------------------
    ternary    := logic_or ('?' ternary ':' ternary)?
    logic_or   := logic_and ('||' logic_and)*
    logic_and  := equality ('&&' equality)*
    equality   := relational (('=='|'!=') relational)*
    relational := additive (('<'|'>'|'<='|'>=') additive)*
    additive   := multiplicative (('+'|'-') multiplicative)*
    multiplicative := unary (('*'|'/'|'%') unary)*
    unary      := ('!'|'-')? postfix
    postfix    := cast? primary
    primary    := NUMBER | STRING | IDENT | call | '(' ternary ')'
    call       := IDENT '(' (ternary (',' ternary)*)? ')'
    cast       := '(' 'DT_' IDENT (',' NUMBER)* ')'

`+` IS OVERLOADED ON PURPOSE, LIKE SSIS'S OWN
-----------------------------------------------
SSIS uses `+` for both arithmetic and string concatenation, disambiguated by
operand type -- exactly what T-SQL does and Calcite does not (Calcite's `+`
is arithmetic only; concatenation is `||`). Real type inference is out of
scope here, so a bounded heuristic stands in for it: `+` becomes `||` if
either side is a string literal or a cast to a string type (DT_WSTR/DT_STR),
and stays `+` otherwise. This covers the concatenation patterns that actually
appear in SSIS packages (building a composite key, tagging a literal reason)
without pretending to type-check the expression.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

TOKEN_RE = re.compile(r"""
    \s*(?:
        (?P<num>\d+\.\d+|\d+)
      | (?P<str>"(?:[^"\\]|\\.)*")
      | (?P<op>==|!=|>=|<=|&&|\|\||[()+\-*/%<>!,?:])
      | (?P<ident>[A-Za-z_][A-Za-z0-9_]*)
    )
""", re.VERBOSE)

STRING_CASTS = {"DT_WSTR", "DT_STR"}
KNOWN_CASTS = {
    "DT_I4": "INTEGER", "DT_I2": "SMALLINT", "DT_I8": "BIGINT",
    "DT_R4": "FLOAT", "DT_R8": "DOUBLE",
    "DT_WSTR": "VARCHAR", "DT_STR": "VARCHAR",
    "DT_BOOL": "BOOLEAN",
}
KNOWN_FUNCTIONS = {"UPPER", "LOWER", "TRIM", "LTRIM", "RTRIM", "LEN", "ISNULL"}


class ExprUnsupported(Exception):
    """The expression uses a construct outside the supported subset.

    Raised rather than emitting a best-guess SQL fragment -- an unsupported
    predicate translated wrong is a silent bug in generated business logic,
    which is worse than refusing outright (see catalog/support.py's REFUSED
    components for the same principle applied at the component level).
    """


# -- AST ----------------------------------------------------------------

@dataclass
class Node:
    pass


@dataclass
class Num(Node):
    text: str


@dataclass
class Str(Node):
    text: str  # includes the SSIS double quotes


@dataclass
class Ident(Node):
    name: str


@dataclass
class Call(Node):
    name: str
    args: list[Node] = field(default_factory=list)


@dataclass
class Cast(Node):
    sql_type: str
    is_string: bool
    operand: Node = None


@dataclass
class UnaryOp(Node):
    op: str
    operand: Node = None


@dataclass
class BinOp(Node):
    op: str
    left: Node = None
    right: Node = None


@dataclass
class Ternary(Node):
    cond: Node = None
    if_true: Node = None
    if_false: Node = None


# -- tokenizer ------------------------------------------------------------

def _tokenize(text: str) -> list[tuple[str, str]]:
    tokens: list[tuple[str, str]] = []
    pos = 0
    while pos < len(text):
        m = TOKEN_RE.match(text, pos)
        if not m or m.end() == pos:
            if text[pos:].strip() == "":
                break
            raise ExprUnsupported(f"unrecognised character at {text[pos:pos+20]!r}")
        pos = m.end()
        kind = m.lastgroup
        value = m.group(kind)
        tokens.append((kind, value))
    return tokens


class _Parser:
    def __init__(self, tokens: list[tuple[str, str]], source: str):
        self.tokens = tokens
        self.i = 0
        self.source = source

    def _peek(self) -> tuple[str, str] | None:
        return self.tokens[self.i] if self.i < len(self.tokens) else None

    def _next(self) -> tuple[str, str]:
        tok = self._peek()
        if tok is None:
            raise ExprUnsupported(f"unexpected end of expression in {self.source!r}")
        self.i += 1
        return tok

    def _expect_op(self, op: str) -> None:
        kind, value = self._next()
        if kind != "op" or value != op:
            raise ExprUnsupported(f"expected {op!r} in {self.source!r}, got {value!r}")

    def parse(self) -> Node:
        node = self._ternary()
        if self._peek() is not None:
            kind, value = self._peek()
            raise ExprUnsupported(f"unexpected trailing {value!r} in {self.source!r}")
        return node

    def _ternary(self) -> Node:
        cond = self._logic_or()
        tok = self._peek()
        if tok and tok == ("op", "?"):
            self._next()
            if_true = self._ternary()
            self._expect_op(":")
            if_false = self._ternary()
            return Ternary(cond, if_true, if_false)
        return cond

    def _binary_level(self, ops: set[str], next_level) -> Node:
        node = next_level()
        while True:
            tok = self._peek()
            if tok and tok[0] == "op" and tok[1] in ops:
                op = self._next()[1]
                node = BinOp(op, node, next_level())
            else:
                return node

    def _logic_or(self) -> Node:
        return self._binary_level({"||"}, self._logic_and)

    def _logic_and(self) -> Node:
        return self._binary_level({"&&"}, self._equality)

    def _equality(self) -> Node:
        return self._binary_level({"==", "!="}, self._relational)

    def _relational(self) -> Node:
        return self._binary_level({"<", ">", "<=", ">="}, self._additive)

    def _additive(self) -> Node:
        return self._binary_level({"+", "-"}, self._multiplicative)

    def _multiplicative(self) -> Node:
        return self._binary_level({"*", "/", "%"}, self._unary)

    def _unary(self) -> Node:
        tok = self._peek()
        if tok and tok == ("op", "!"):
            self._next()
            return UnaryOp("!", self._unary())
        if tok and tok == ("op", "-"):
            self._next()
            return UnaryOp("-", self._unary())
        return self._postfix()

    def _postfix(self) -> Node:
        cast = self._try_cast()
        if cast is not None:
            cast.operand = self._unary()
            return cast
        return self._primary()

    def _try_cast(self) -> Cast | None:
        """`(DT_I4)` / `(DT_WSTR,10)` -- a parenthesised SSIS type name,
        distinguished from a grouping paren by the DT_ prefix."""
        save = self.i
        tok = self._peek()
        if not (tok and tok == ("op", "(")):
            return None
        self._next()
        tok = self._peek()
        if not (tok and tok[0] == "ident" and tok[1].startswith("DT_")):
            self.i = save
            return None
        type_name = self._next()[1]
        if type_name not in KNOWN_CASTS:
            raise ExprUnsupported(f"unsupported cast type {type_name!r} in {self.source!r}")
        while self._peek() and self._peek() == ("op", ","):
            self._next()
            self._next()  # length/precision/scale argument, not needed in Calcite
        self._expect_op(")")
        return Cast(KNOWN_CASTS[type_name], type_name in STRING_CASTS)

    def _primary(self) -> Node:
        tok = self._next()
        kind, value = tok
        if kind == "num":
            return Num(value)
        if kind == "str":
            return Str(value)
        if kind == "op" and value == "(":
            node = self._ternary()
            self._expect_op(")")
            return node
        if kind == "ident":
            if self._peek() and self._peek() == ("op", "("):
                if value.upper() not in KNOWN_FUNCTIONS:
                    raise ExprUnsupported(f"unsupported function {value!r} in {self.source!r}")
                self._next()
                args = []
                if not (self._peek() and self._peek() == ("op", ")")):
                    args.append(self._ternary())
                    while self._peek() and self._peek() == ("op", ","):
                        self._next()
                        args.append(self._ternary())
                self._expect_op(")")
                return Call(value.upper(), args)
            return Ident(value)
        raise ExprUnsupported(f"unexpected token {value!r} in {self.source!r}")


def parse(text: str) -> Node:
    return _Parser(_tokenize(text), text).parse()


# -- SQL emission -----------------------------------------------------------

_SQL_OP = {"==": "=", "!=": "<>", "&&": "AND", "||": "OR"}
_STRING_FUNCS = {"UPPER", "LOWER", "TRIM", "LTRIM", "RTRIM"}


def _is_stringy(node: Node) -> bool:
    """Best-effort guess at whether a subexpression is string-typed --
    enough to resolve SSIS's overloaded `+` (see module docstring), not a
    real type checker."""
    if isinstance(node, Str):
        return True
    if isinstance(node, Cast):
        return node.is_string
    if isinstance(node, Call):
        return node.name in _STRING_FUNCS
    if isinstance(node, BinOp) and node.op == "+":
        return _is_stringy(node.left) or _is_stringy(node.right)
    return False


def _emit(node: Node) -> str:
    if isinstance(node, Num):
        return node.text
    if isinstance(node, Str):
        # SSIS double-quoted -> SQL single-quoted. The tokenizer only accepts
        # \" and \\ escapes inside a string literal; both survive unescaping
        # a Python literal and re-quoting for SQL.
        inner = node.text[1:-1].replace('\\"', '"').replace("\\\\", "\\")
        return "'" + inner.replace("'", "''") + "'"
    if isinstance(node, Ident):
        return node.name
    if isinstance(node, Call):
        if node.name == "ISNULL":
            if len(node.args) != 1:
                raise ExprUnsupported("ISNULL takes exactly one argument")
            return f"({_emit(node.args[0])} IS NULL)"
        return f"{node.name}({', '.join(_emit(a) for a in node.args)})"
    if isinstance(node, Cast):
        return f"CAST({_emit(node.operand)} AS {node.sql_type})"
    if isinstance(node, UnaryOp):
        if node.op == "!":
            return f"(NOT {_emit(node.operand)})"
        return f"(-{_emit(node.operand)})"
    if isinstance(node, BinOp):
        op = node.op
        if op == "+" and (_is_stringy(node.left) or _is_stringy(node.right)):
            sql_op = "||"
        else:
            sql_op = _SQL_OP.get(op, op)
        return f"({_emit(node.left)} {sql_op} {_emit(node.right)})"
    if isinstance(node, Ternary):
        return (f"(CASE WHEN {_emit(node.cond)} THEN {_emit(node.if_true)} "
                f"ELSE {_emit(node.if_false)} END)")
    raise ExprUnsupported(f"internal: no emitter for {node!r}")


def translate(ssis_expression: str) -> str:
    """SSIS expression text -> a Calcite SQL fragment. Raises ExprUnsupported
    for anything outside the documented subset."""
    return _emit(parse(ssis_expression))
