use std::{io, iter, iter::Fuse, time::Duration};

use socket2::Socket;

use crate::topology::node::PoolSize;

#[derive(derivative::Derivative)]
#[derivative(Debug)]
#[non_exhaustive]
pub struct ConnectionConfig {
    pub buffer_size: usize,
    pub connect_timeout: Duration,
    pub heartbeat_interval: Option<Duration>,
    #[derivative(Debug = "ignore")]
    pub init_socket: Box<dyn InitSocket>,
    pub pool_size: PoolSize,
    #[derivative(Debug = "ignore")]
    pub reconnection_policy: Box<dyn ReconnectionPolicy>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1 << 13,
            connect_timeout: Duration::from_secs(5),
            heartbeat_interval: None,
            init_socket: Box::new(|socket: &mut Socket| -> io::Result<()> {
                socket.set_nodelay(true)?;
                socket.set_keepalive(true)?;
                Ok(())
            }),
            pool_size: PoolSize::default(),
            reconnection_policy: Box::new(|| iter::repeat(Duration::from_secs(1))),
        }
    }
}

impl ConnectionConfig {
    fn new() -> Self {
        Self::default()
    }

    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = Some(interval);
        self
    }

    pub fn init_socket(mut self, opt: impl InitSocket + 'static) -> Self {
        self.init_socket = Box::new(opt);
        self
    }

    pub fn pool_size(mut self, pool_size: PoolSize) -> Self {
        self.pool_size = pool_size;
        self
    }

    pub fn reconnection_policy(
        mut self,
        reconnection_policy: impl ReconnectionPolicy + 'static,
    ) -> Self {
        self.reconnection_policy = Box::new(reconnection_policy);
        self
    }

    pub fn retry_exponential(
        self,
        mut initial_delay: Duration,
        max_delay: Duration,
        mult: u32,
    ) -> Self {
        self.reconnection_policy(move || {
            iter::repeat_with(move || {
                let delay = initial_delay;
                initial_delay = (initial_delay * mult).max(max_delay);
                delay
            })
        })
    }

    pub fn retry_interval(self, interval: Duration) -> Self {
        self.reconnection_policy(move || iter::repeat(interval))
    }
}

pub trait InitSocket: Send + Sync {
    fn initialize_socket(&self, socket: &mut Socket) -> io::Result<()>;
}

impl<F> InitSocket for F
where
    F: Send + Sync + Fn(&mut Socket) -> io::Result<()>,
{
    fn initialize_socket(&self, socket: &mut Socket) -> io::Result<()> {
        self(socket)
    }
}

pub trait ReconnectionPolicy: Send + Sync {
    fn reconnection_delays(&self) -> Box<dyn Iterator<Item = Duration> + Send + Sync>;
}

impl<F, I> ReconnectionPolicy for F
where
    F: Send + Sync + Fn() -> I,
    I: Iterator<Item = Duration> + Send + Sync + 'static,
{
    fn reconnection_delays(&self) -> Box<dyn Iterator<Item = Duration> + Send + Sync> {
        Box::new(self())
    }
}

impl ReconnectionPolicy for Duration {
    fn reconnection_delays(&self) -> Box<dyn Iterator<Item = Duration> + Send + Sync> {
        return Box::new(iter::repeat(*self));
    }
}

pub(crate) struct ReconnectionDelays {
    iter: Fuse<Box<dyn Iterator<Item = Duration>>>,
    last: Option<Duration>,
}

impl ReconnectionDelays {
    pub(crate) fn new(iter: Box<dyn Iterator<Item = Duration>>) -> Self {
        Self {
            iter: iter.fuse(),
            last: None,
        }
    }

    pub(crate) fn next_delay(&mut self) -> Duration {
        if let Some(next) = self.iter.next() {
            self.last = Some(next);
            next
        } else {
            self.last.expect("Reconnection delays must not be empty")
        }
    }
}
