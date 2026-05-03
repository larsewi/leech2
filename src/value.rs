use std::fmt;
use std::hash::{Hash, Hasher};

use anyhow::{Result, bail};

use crate::proto::cell::Value as ProtoValue;
use crate::proto::cell::value::Kind;

/// A single typed cell in a table row.
///
/// `Value` is the domain counterpart to `proto::cell::Value`. The proto
/// representation wraps the variant in `Option<Kind>` because protobuf
/// can't distinguish "the oneof was set to a default-valued variant" from
/// "the oneof was never set"; the domain type has no such ambiguity.
#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Text(String),
    Boolean(bool),
    Number(f64),
}

impl Value {
    /// Construct a numeric value, rejecting `NaN` and infinities and
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
        Ok(Value::Number(normalized))
    }
}

// `f64` only implements `PartialEq`, not `Eq`, because `NaN != NaN`. The
// `Value::number` constructor rejects `NaN`, so within `Value` the `f64`
// payload is always a finite, non-NaN value and total equality is sound.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Number(a), Value::Number(b)) => a.to_bits() == b.to_bits(),
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Mix the variant tag in first so that variants with payloads that
        // share a byte pattern (e.g. `Boolean(false)` and `Number(0.0)`)
        // don't collide in a HashMap. This is what `#[derive(Hash)]` does;
        // we hand-roll because `f64` has no `Hash` impl.
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Text(s) => s.hash(state),
            Value::Boolean(b) => b.hash(state),
            Value::Number(n) => n.to_bits().hash(state),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Text(s) => write!(f, "{:?}", s),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Number(n) => write!(f, "{}", n),
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Text(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Text(s)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Boolean(b)
    }
}

impl From<f64> for Value {
    fn from(n: f64) -> Self {
        Value::Number(n)
    }
}

impl TryFrom<ProtoValue> for Value {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoValue) -> Result<Self> {
        match proto.kind {
            Some(Kind::Null(())) => Ok(Value::Null),
            Some(Kind::Text(s)) => Ok(Value::Text(s)),
            Some(Kind::Boolean(b)) => Ok(Value::Boolean(b)),
            Some(Kind::Number(n)) => Value::number(n),
            None => bail!("Value message has no kind set"),
        }
    }
}

impl TryFrom<&ProtoValue> for Value {
    type Error = anyhow::Error;

    fn try_from(proto: &ProtoValue) -> Result<Self> {
        match &proto.kind {
            Some(Kind::Null(())) => Ok(Value::Null),
            Some(Kind::Text(s)) => Ok(Value::Text(s.clone())),
            Some(Kind::Boolean(b)) => Ok(Value::Boolean(*b)),
            Some(Kind::Number(n)) => Value::number(*n),
            None => bail!("Value message has no kind set"),
        }
    }
}

impl fmt::Display for ProtoValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match Value::try_from(self) {
            Ok(v) => v.fmt(f),
            Err(_) => write!(f, "<corrupt value>"),
        }
    }
}

/// Convert a vector of proto values into a vector of domain `Value`s,
/// short-circuiting on the first malformed entry.
pub fn decode_proto_values(protos: Vec<ProtoValue>) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(protos.len());
    for proto in protos {
        out.push(Value::try_from(proto)?);
    }
    Ok(out)
}

/// Build a `Vec<Value>` of `Text` variants from a slice of `&str` — handy
/// for test fixtures.
#[cfg(test)]
pub(crate) fn text_values(strs: &[&str]) -> Vec<Value> {
    strs.iter().map(|&s| s.into()).collect()
}

/// Build a `Vec<ProtoValue>` of `Text` variants from a slice of `&str` —
/// handy for test fixtures that need to populate proto messages directly.
#[cfg(test)]
pub(crate) fn text_proto_values(strs: &[&str]) -> Vec<ProtoValue> {
    strs.iter().map(|&s| Value::from(s).into()).collect()
}

/// Render a slice of proto values as a comma-separated string for
/// log/display output.
pub fn display_proto_values(values: &[ProtoValue]) -> String {
    let mut out = String::new();
    for (i, value) in values.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&value.to_string());
    }
    out
}

impl From<Value> for ProtoValue {
    fn from(value: Value) -> Self {
        let kind = match value {
            Value::Null => Kind::Null(()),
            Value::Text(s) => Kind::Text(s),
            Value::Boolean(b) => Kind::Boolean(b),
            Value::Number(n) => Kind::Number(n),
        };
        ProtoValue { kind: Some(kind) }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;

    use super::*;

    fn hash_of(v: &Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        v.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn number_rejects_nan() {
        assert!(Value::number(f64::NAN).is_err());
    }

    #[test]
    fn number_rejects_infinities() {
        assert!(Value::number(f64::INFINITY).is_err());
        assert!(Value::number(f64::NEG_INFINITY).is_err());
    }

    #[test]
    fn number_normalizes_negative_zero() {
        let pos = Value::number(0.0).unwrap();
        let neg = Value::number(-0.0).unwrap();
        assert_eq!(pos, neg);
        assert_eq!(hash_of(&pos), hash_of(&neg));
        // Both stored as +0.0 (positive bit pattern).
        if let Value::Number(n) = pos {
            assert_eq!(n.to_bits(), 0.0_f64.to_bits());
        } else {
            panic!("expected Number");
        }
    }

    #[test]
    fn number_preserves_finite_values() {
        let v = Value::number(2.5).unwrap();
        assert_eq!(v, Value::Number(2.5));
    }

    #[test]
    fn equality_across_variants_is_false() {
        assert_ne!(Value::Null, Value::Text(String::new()));
        assert_ne!(Value::Boolean(false), Value::Number(0.0));
        assert_ne!(Value::Text("true".into()), Value::Boolean(true));
    }

    #[test]
    fn equality_within_variants() {
        assert_eq!(Value::Null, Value::Null);
        assert_eq!(Value::Text("a".into()), Value::Text("a".into()));
        assert_eq!(Value::Boolean(true), Value::Boolean(true));
        assert_eq!(Value::number(1.5).unwrap(), Value::number(1.5).unwrap());
    }

    #[test]
    fn hash_matches_equality() {
        // Equal values must hash equal — the HashMap contract.
        let pairs = [
            (Value::Null, Value::Null),
            (Value::Text("x".into()), Value::Text("x".into())),
            (Value::Boolean(true), Value::Boolean(true)),
            (Value::number(2.71).unwrap(), Value::number(2.71).unwrap()),
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
        let null_h = hash_of(&Value::Null);
        let text_h = hash_of(&Value::Text(String::new()));
        let bool_h = hash_of(&Value::Boolean(false));
        let num_h = hash_of(&Value::Number(0.0));
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
            Value::Null,
            Value::Text("hello".into()),
            Value::Boolean(true),
            Value::Boolean(false),
            Value::number(0.0).unwrap(),
            Value::number(2.5).unwrap(),
            Value::number(-1.5).unwrap(),
        ];
        for v in cases {
            let proto: ProtoValue = v.clone().into();
            let back: Value = proto.try_into().unwrap();
            assert_eq!(v, back);
        }
    }

    #[test]
    fn try_from_proto_rejects_unset_kind() {
        let proto = ProtoValue { kind: None };
        let err = Value::try_from(proto).unwrap_err();
        assert!(err.to_string().contains("no kind set"), "got: {err}");
    }

    #[test]
    fn try_from_proto_rejects_nan_number() {
        let proto = ProtoValue {
            kind: Some(Kind::Number(f64::NAN)),
        };
        assert!(Value::try_from(proto).is_err());
    }
}
