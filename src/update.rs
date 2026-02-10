pub use crate::proto::update::Update;

use std::fmt;

use crate::utils::format_row;

impl fmt::Display for Update {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {} -> {}",
            format_row(&self.key),
            format_row(&self.old_value),
            format_row(&self.new_value)
        )
    }
}
