use std::{fmt, num::NonZeroUsize, time::Duration};

pub trait SpeculativeExecutionPolicy: fmt::Debug + Send + Sync {
    fn next_delay(&self, running_executions: usize) -> Option<Duration>;
}

#[derive(Debug)]
pub struct ConstantSpeculativeExecutionPolicy {
    delay: Duration,
    max_executions: NonZeroUsize,
}

impl ConstantSpeculativeExecutionPolicy {
    pub fn new(delay: Duration, max_executions: NonZeroUsize) -> Self {
        Self {
            delay,
            max_executions,
        }
    }
}

impl SpeculativeExecutionPolicy for ConstantSpeculativeExecutionPolicy {
    fn next_delay(&self, running_executions: usize) -> Option<Duration> {
        (running_executions < self.max_executions.get()).then_some(self.delay)
    }
}
