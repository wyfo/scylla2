use std::{fmt, sync::Arc};

use crate::{
    topology::{node::Node, ring::Partition},
    Session,
};

pub trait DynamicLoadBalancingPolicy: fmt::Debug + Send + Sync {
    fn query_plan<'a>(
        &self,
        session: &'a Session,
        partition: Option<&'a Partition>,
        is_lwt: bool,
    ) -> Box<dyn Iterator<Item = &'a Arc<Node>> + 'a>;
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum LoadBalancingPolicy {
    #[default]
    TokenAware,
    Dynamic(Arc<dyn DynamicLoadBalancingPolicy>),
}

impl<T> From<T> for LoadBalancingPolicy
where
    T: DynamicLoadBalancingPolicy + 'static,
{
    fn from(value: T) -> Self {
        LoadBalancingPolicy::Dynamic(Arc::new(value))
    }
}

impl<T> From<Arc<T>> for LoadBalancingPolicy
where
    T: DynamicLoadBalancingPolicy + 'static,
{
    fn from(value: Arc<T>) -> Self {
        LoadBalancingPolicy::Dynamic(value)
    }
}
