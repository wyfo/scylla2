use std::fmt;

/// Wrapper implementing fmt::Debug for closures
pub struct Closure<F>(pub F);

impl<F> fmt::Debug for Closure<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<closure>")
    }
}
