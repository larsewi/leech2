pub use crate::proto::entry::Entry;

use std::fmt;

use crate::utils::format_row;

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} -> {}", format_row(&self.key), format_row(&self.value))
    }
}
