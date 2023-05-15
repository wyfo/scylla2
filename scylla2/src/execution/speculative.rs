use std::fmt;

pub trait SpeculativeExecutionPolicy: fmt::Debug + Send + Sync {}
