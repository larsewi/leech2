use std::fmt;
use std::hash::{Hash, Hasher};

use anyhow::{Context, Result, bail};

use crate::proto::cell::Cell as ProtoCell;
use crate::proto::cell::cell::Kind;

/// A single typed value at one (row, column) in a table.
///
/// `Cell` is the domain counterpart to `proto::cell::Cell`. The proto
/// representation wraps the variant in `Option<Kind>` because protobuf
/// can't distinguish "the oneof was set to a default-valued variant" from
/// "the oneof was never set"; the domain type has no such ambiguity.
#[derive(Clone, Debug)]
pub enum Cell {
    Null,
    Text(String),
    Boolean(bool),
    Number(f64),
}

impl Cell {
    /// Construct a numeric cell, rejecting `NaN` and infinities and
    /// normalizing `-0.0` to `0.0` so that bitwise hashing matches
    /// arithmetic equality.
    pub fn number(n: f64) -> Result<Self> {
        if n.is_nan() {
            bail!("invalid number: NaN");
        }
        if n.is_infinite() {
            bail!("invalid number: infinity");
        }
        let normalized = if n == 0.0 { 0.0 } else { n };
        Ok(Cell::Number(normalized))
    }

    pub fn kind(&self) -> ValueKind {
        match self {
            Cell::Null => ValueKind::Null,
            Cell::Text(_) => ValueKind::Text,
            Cell::Boolean(_) => ValueKind::Boolean,
            Cell::Number(_) => ValueKind::Number,
        }
    }
}

/// The variant tag of a [`Cell`], without the payload. Used to declare a
/// field's expected type in config and to validate that a wire cell's
/// variant matches that declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    Null,
    Text,
    Number,
    Boolean,
}

// `f64` only implements `PartialEq`, not `Eq`, because `NaN != NaN`. The
// `Cell::number` constructor rejects `NaN`, so within `Cell` the `f64`
// payload is always a finite, non-NaN value and total equality is sound.
impl PartialEq for Cell {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Cell::Null, Cell::Null) => true,
            (Cell::Text(a), Cell::Text(b)) => a == b,
            (Cell::Boolean(a), Cell::Boolean(b)) => a == b,
            (Cell::Number(a), Cell::Number(b)) => a.to_bits() == b.to_bits(),
            _ => false,
        }
    }
}

impl Eq for Cell {}

impl Hash for Cell {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Mix the variant tag in first so that variants with payloads that
        // share a byte pattern (e.g. `Boolean(false)` and `Number(0.0)`)
        // don't collide in a HashMap. This is what `#[derive(Hash)]` does;
        // we hand-roll because `f64` has no `Hash` impl.
        std::mem::discriminant(self).hash(state);
        match self {
            Cell::Null => {}
            Cell::Text(s) => s.hash(state),
            Cell::Boolean(b) => b.hash(state),
            Cell::Number(n) => n.to_bits().hash(state),
        }
    }
}

impl fmt::Display for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cell::Null => write!(f, "NULL"),
            Cell::Text(s) => write!(f, "{:?}", s),
            Cell::Boolean(b) => write!(f, "{}", b),
            Cell::Number(n) => write!(f, "{}", n),
        }
    }
}

impl From<&str> for Cell {
    fn from(s: &str) -> Self {
        Cell::Text(s.to_string())
    }
}

impl From<String> for Cell {
    fn from(s: String) -> Self {
        Cell::Text(s)
    }
}

impl From<bool> for Cell {
    fn from(b: bool) -> Self {
        Cell::Boolean(b)
    }
}

impl From<f64> for Cell {
    fn from(n: f64) -> Self {
        Cell::Number(n)
    }
}

impl TryFrom<ProtoCell> for Cell {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoCell) -> Result<Self> {
        match proto.kind {
            Some(Kind::Null(())) => Ok(Cell::Null),
            Some(Kind::Text(s)) => Ok(Cell::Text(s)),
            Some(Kind::Boolean(b)) => Ok(Cell::Boolean(b)),
            Some(Kind::Number(n)) => Cell::number(n),
            None => bail!("Cell message has no kind set"),
        }
    }
}

impl TryFrom<&ProtoCell> for Cell {
    type Error = anyhow::Error;

    fn try_from(proto: &ProtoCell) -> Result<Self> {
        match &proto.kind {
            Some(Kind::Null(())) => Ok(Cell::Null),
            Some(Kind::Text(s)) => Ok(Cell::Text(s.clone())),
            Some(Kind::Boolean(b)) => Ok(Cell::Boolean(*b)),
            Some(Kind::Number(n)) => Cell::number(*n),
            None => bail!("Cell message has no kind set"),
        }
    }
}

impl fmt::Display for ProtoCell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match Cell::try_from(self) {
            Ok(v) => v.fmt(f),
            Err(_) => write!(f, "<corrupt cell>"),
        }
    }
}

/// Convert a vector of proto cells into a vector of domain `Cell`s,
/// short-circuiting on the first malformed entry.
pub fn decode_proto_cells(protos: Vec<ProtoCell>) -> Result<Vec<Cell>> {
    let mut out = Vec::with_capacity(protos.len());
    for proto in protos {
        out.push(Cell::try_from(proto)?);
    }
    Ok(out)
}

/// Build a `Vec<Cell>` of `Text` variants from a slice of `&str` — handy
/// for test fixtures.
#[cfg(test)]
pub(crate) fn text_cells(strs: &[&str]) -> Vec<Cell> {
    strs.iter().map(|&s| s.into()).collect()
}

/// Build a `Vec<ProtoCell>` of `Text` variants from a slice of `&str` —
/// handy for test fixtures that need to populate proto messages directly.
#[cfg(test)]
pub(crate) fn text_proto_cells(strs: &[&str]) -> Vec<ProtoCell> {
    strs.iter().map(|&s| Cell::from(s).into()).collect()
}

/// Render a slice of proto cells as a comma-separated string for
/// log/display output.
pub fn display_proto_cells(cells: &[ProtoCell]) -> String {
    let mut out = String::new();
    for (i, cell) in cells.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&cell.to_string());
    }
    out
}

impl From<Cell> for ProtoCell {
    fn from(cell: Cell) -> Self {
        let kind = match cell {
            Cell::Null => Kind::Null(()),
            Cell::Text(s) => Kind::Text(s),
            Cell::Boolean(b) => Kind::Boolean(b),
            Cell::Number(n) => Kind::Number(n),
        };
        ProtoCell { kind: Some(kind) }
    }
}

impl ValueKind {
    /// Parse a config field's `type` string (`"TEXT"` / `"NUMBER"` /
    /// `"BOOLEAN"`, case-insensitive) into a [`ValueKind`]. Config never
    /// declares NULL as a type, so [`ValueKind::Null`] is not produced
    /// here.
    pub fn from_config(type_str: &str) -> Result<Self> {
        match type_str.to_uppercase().as_str() {
            "TEXT" => Ok(ValueKind::Text),
            "NUMBER" => Ok(ValueKind::Number),
            "BOOLEAN" => Ok(ValueKind::Boolean),
            other => bail!(
                "unknown field type '{}'; valid types are: TEXT, NUMBER, BOOLEAN",
                other
            ),
        }
    }
}

/// Default sentinel matched as boolean true when no per-field override is set.
pub const DEFAULT_TRUE_SENTINEL: &str = "true";
/// Default sentinel matched as boolean false when no per-field override is set.
pub const DEFAULT_FALSE_SENTINEL: &str = "false";

/// Parse a boolean string with strict, case-sensitive equality against the
/// supplied sentinels. Use [`DEFAULT_TRUE_SENTINEL`] / [`DEFAULT_FALSE_SENTINEL`]
/// when no per-field override is configured.
pub fn parse_boolean(value: &str, true_sentinel: &str, false_sentinel: &str) -> Result<bool> {
    if value == true_sentinel {
        Ok(true)
    } else if value == false_sentinel {
        Ok(false)
    } else {
        bail!(
            "invalid boolean value '{}' (expected '{}' or '{}')",
            value,
            true_sentinel,
            false_sentinel
        );
    }
}

/// Parse a string into a typed `Cell` according to the kind tag. Boolean
/// parsing uses the default sentinels; CSV-parsing callers that honor
/// per-field overrides should call [`parse_boolean`] directly. Passing
/// [`ValueKind::Null`] is rejected — Null is set via the field's
/// null-sentinel mechanism, not by parsing.
pub fn parse_typed_cell(value: &str, kind: ValueKind) -> Result<Cell> {
    match kind {
        ValueKind::Null => bail!("cannot parse value as NULL"),
        ValueKind::Text => Ok(Cell::Text(value.to_string())),
        ValueKind::Number => {
            let parsed: f64 = value
                .parse()
                .with_context(|| format!("invalid number: '{}'", value))?;
            Cell::number(parsed)
        }
        ValueKind::Boolean => Ok(Cell::Boolean(parse_boolean(
            value,
            DEFAULT_TRUE_SENTINEL,
            DEFAULT_FALSE_SENTINEL,
        )?)),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;

    use super::*;

    fn hash_of(v: &Cell) -> u64 {
        let mut hasher = DefaultHasher::new();
        v.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn number_rejects_nan() {
        assert!(Cell::number(f64::NAN).is_err());
    }

    #[test]
    fn number_rejects_infinities() {
        assert!(Cell::number(f64::INFINITY).is_err());
        assert!(Cell::number(f64::NEG_INFINITY).is_err());
    }

    #[test]
    fn number_normalizes_negative_zero() {
        let pos = Cell::number(0.0).unwrap();
        let neg = Cell::number(-0.0).unwrap();
        assert_eq!(pos, neg);
        assert_eq!(hash_of(&pos), hash_of(&neg));
        // Both stored as +0.0 (positive bit pattern).
        if let Cell::Number(n) = pos {
            assert_eq!(n.to_bits(), 0.0_f64.to_bits());
        } else {
            panic!("expected Number");
        }
    }

    #[test]
    fn number_preserves_finite_values() {
        let v = Cell::number(2.5).unwrap();
        assert_eq!(v, Cell::Number(2.5));
    }

    #[test]
    fn equality_across_variants_is_false() {
        assert_ne!(Cell::Null, Cell::Text(String::new()));
        assert_ne!(Cell::Boolean(false), Cell::Number(0.0));
        assert_ne!(Cell::Text("true".into()), Cell::Boolean(true));
    }

    #[test]
    fn equality_within_variants() {
        assert_eq!(Cell::Null, Cell::Null);
        assert_eq!(Cell::Text("a".into()), Cell::Text("a".into()));
        assert_eq!(Cell::Boolean(true), Cell::Boolean(true));
        assert_eq!(Cell::number(1.5).unwrap(), Cell::number(1.5).unwrap());
    }

    #[test]
    fn hash_matches_equality() {
        // Equal values must hash equal — the HashMap contract.
        let pairs = [
            (Cell::Null, Cell::Null),
            (Cell::Text("x".into()), Cell::Text("x".into())),
            (Cell::Boolean(true), Cell::Boolean(true)),
            (Cell::number(2.71).unwrap(), Cell::number(2.71).unwrap()),
        ];
        for (a, b) in pairs {
            assert_eq!(a, b);
            assert_eq!(hash_of(&a), hash_of(&b));
        }
    }

    #[test]
    fn hash_distinguishes_variants() {
        // Different variants with the same payload-equivalent value should
        // hash differently — otherwise Boolean(false) and Number(0.0) and
        // Text("") could collide in a HashMap.
        let null_h = hash_of(&Cell::Null);
        let text_h = hash_of(&Cell::Text(String::new()));
        let bool_h = hash_of(&Cell::Boolean(false));
        let num_h = hash_of(&Cell::Number(0.0));
        // At least one pair differs; checking all-distinct is too strict
        // for a hash function, but the discriminant prefix should make
        // collisions extremely unlikely.
        let hashes = [null_h, text_h, bool_h, num_h];
        let unique: std::collections::HashSet<_> = hashes.iter().collect();
        assert_eq!(unique.len(), 4, "expected 4 distinct hashes: {hashes:?}");
    }

    #[test]
    fn proto_round_trip() {
        let cases = [
            Cell::Null,
            Cell::Text("hello".into()),
            Cell::Boolean(true),
            Cell::Boolean(false),
            Cell::number(0.0).unwrap(),
            Cell::number(2.5).unwrap(),
            Cell::number(-1.5).unwrap(),
        ];
        for v in cases {
            let proto: ProtoCell = v.clone().into();
            let back: Cell = proto.try_into().unwrap();
            assert_eq!(v, back);
        }
    }

    #[test]
    fn try_from_proto_rejects_unset_kind() {
        let proto = ProtoCell { kind: None };
        let err = Cell::try_from(proto).unwrap_err();
        assert!(err.to_string().contains("no kind set"), "got: {err}");
    }

    #[test]
    fn try_from_proto_rejects_nan_number() {
        let proto = ProtoCell {
            kind: Some(Kind::Number(f64::NAN)),
        };
        assert!(Cell::try_from(proto).is_err());
    }

    #[test]
    fn test_value_kind_from_config() {
        assert_eq!(ValueKind::from_config("TEXT").unwrap(), ValueKind::Text);
        assert_eq!(ValueKind::from_config("NUMBER").unwrap(), ValueKind::Number);
        assert_eq!(
            ValueKind::from_config("BOOLEAN").unwrap(),
            ValueKind::Boolean
        );
        // Case insensitive
        assert_eq!(ValueKind::from_config("text").unwrap(), ValueKind::Text);
        assert_eq!(ValueKind::from_config("number").unwrap(), ValueKind::Number);
        assert_eq!(
            ValueKind::from_config("Boolean").unwrap(),
            ValueKind::Boolean
        );
        // Unknown types are rejected
        assert!(ValueKind::from_config("unknown").is_err());
        // NULL is not a valid declared type
        assert!(ValueKind::from_config("NULL").is_err());
    }

    #[test]
    fn test_value_kind_matches_cell() {
        assert_eq!(Cell::Null.kind(), ValueKind::Null);
        assert_eq!(Cell::Text("x".into()).kind(), ValueKind::Text);
        assert_eq!(Cell::Number(1.0).kind(), ValueKind::Number);
        assert_eq!(Cell::Boolean(true).kind(), ValueKind::Boolean);
    }

    #[test]
    fn test_parse_typed_cell_rejects_null_kind() {
        assert!(parse_typed_cell("anything", ValueKind::Null).is_err());
    }

    #[test]
    fn test_parse_boolean_default_sentinels() {
        assert!(parse_boolean("true", DEFAULT_TRUE_SENTINEL, DEFAULT_FALSE_SENTINEL).unwrap());
        assert!(!parse_boolean("false", DEFAULT_TRUE_SENTINEL, DEFAULT_FALSE_SENTINEL).unwrap());
    }

    #[test]
    fn test_parse_boolean_default_sentinels_are_case_sensitive() {
        for input in ["True", "TRUE", "False", "FALSE"] {
            assert!(
                parse_boolean(input, DEFAULT_TRUE_SENTINEL, DEFAULT_FALSE_SENTINEL).is_err(),
                "input '{input}' should be rejected under strict default sentinels"
            );
        }
    }

    #[test]
    fn test_parse_boolean_legacy_synonyms_no_longer_accepted() {
        for input in ["1", "0", "t", "f", "yes", "no"] {
            assert!(
                parse_boolean(input, DEFAULT_TRUE_SENTINEL, DEFAULT_FALSE_SENTINEL).is_err(),
                "input '{input}' should no longer be accepted"
            );
        }
    }

    #[test]
    fn test_parse_boolean_custom_sentinels() {
        assert!(parse_boolean("Y", "Y", "N").unwrap());
        assert!(!parse_boolean("N", "Y", "N").unwrap());
        // The defaults are not honoured when custom sentinels are in use.
        assert!(parse_boolean("true", "Y", "N").is_err());
        assert!(parse_boolean("false", "Y", "N").is_err());
    }

    #[test]
    fn test_parse_boolean_rejects_invalid() {
        assert!(parse_boolean("maybe", DEFAULT_TRUE_SENTINEL, DEFAULT_FALSE_SENTINEL).is_err());
        assert!(parse_boolean("", DEFAULT_TRUE_SENTINEL, DEFAULT_FALSE_SENTINEL).is_err());
    }
}
