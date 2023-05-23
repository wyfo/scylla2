use std::{sync::Arc, time::Duration};

use scylla2_cql::{Consistency, SerialConsistency};

use crate::execution::{
    load_balancing::LoadBalancingPolicy,
    retry::{DefaultRetryPolicy, RetryPolicy},
    speculative::SpeculativeExecutionPolicy,
};

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ExecutionProfile {
    pub consistency: Consistency,
    pub serial_consistency: Option<SerialConsistency>,
    pub request_timeout: Option<Duration>,
    pub tracing: bool,
    pub load_balancing_policy: LoadBalancingPolicy,
    pub retry_policy: Arc<dyn RetryPolicy>,
    pub speculative_execution_policy: Option<Arc<dyn SpeculativeExecutionPolicy>>,
}

impl Default for ExecutionProfile {
    fn default() -> Self {
        Self {
            consistency: Default::default(),
            serial_consistency: Default::default(),
            request_timeout: Some(Duration::from_secs(30)),
            tracing: Default::default(),
            load_balancing_policy: Default::default(),
            retry_policy: Arc::new(DefaultRetryPolicy),
            speculative_execution_policy: Default::default(),
        }
    }
}

impl ExecutionProfile {
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.consistency = consistency;
        self
    }

    pub fn serial_consistency(
        mut self,
        serial_consistency: impl Into<Option<SerialConsistency>>,
    ) -> Self {
        self.serial_consistency = serial_consistency.into();
        self
    }

    pub fn request_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.request_timeout = timeout.into();
        self
    }

    pub fn load_balancing_policy(mut self, policy: impl Into<LoadBalancingPolicy>) -> Self {
        self.load_balancing_policy = policy.into();
        self
    }

    pub fn retry_policy(mut self, policy: impl RetryPolicy + 'static) -> Self {
        self.retry_policy = Arc::new(policy);
        self
    }

    pub fn speculative_execution_policy(
        mut self,
        policy: impl SpeculativeExecutionPolicy + 'static,
    ) -> Self {
        self.speculative_execution_policy = Some(Arc::new(policy));
        self
    }
}
