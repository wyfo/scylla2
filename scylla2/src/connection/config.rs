use std::{fmt, io, iter, time::Duration};

use socket2::Socket;

use crate::{debug::Closure, topology::node::PoolSize};

#[derive(Debug)]
#[non_exhaustive]
pub struct ConnectionConfig {
    pub connect_timeout: Duration,
    pub heartbeat_interval: Option<Duration>,
    pub init_socket: Box<dyn InitSocket>,
    pub pool_size: PoolSize,
    pub read_buffer_size: usize,
    pub reconnection_policy: Box<dyn ReconnectionPolicy>,
    pub write_buffer_size: usize,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            heartbeat_interval: None,
            init_socket: Box::new(NoDelayAndKeepAlive),
            pool_size: PoolSize::default(),
            read_buffer_size: 8 * 1024,
            reconnection_policy: Box::new(Duration::from_secs(1)),
            write_buffer_size: 8 * 1024,
        }
    }
}

impl ConnectionConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn heartbeat_interval(mut self, interval: impl Into<Option<Duration>>) -> Self {
        self.heartbeat_interval = interval.into();
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

    pub fn read_buffer_size(mut self, size: usize) -> Self {
        self.read_buffer_size = size;
        self
    }

    pub fn reconnection_policy(
        mut self,
        reconnection_policy: impl ReconnectionPolicy + 'static,
    ) -> Self {
        self.reconnection_policy = Box::new(reconnection_policy);
        self
    }

    pub fn write_buffer_size(mut self, size: usize) -> Self {
        self.write_buffer_size = size;
        self
    }
}

pub trait InitSocket: fmt::Debug + Send + Sync {
    fn initialize_socket(&self, socket: &mut Socket) -> io::Result<()>;
}

impl<F> InitSocket for Closure<F>
where
    F: Send + Sync + Fn(&mut Socket) -> io::Result<()>,
{
    fn initialize_socket(&self, socket: &mut Socket) -> io::Result<()> {
        self.0(socket)
    }
}

#[derive(Debug)]
struct NoDelayAndKeepAlive;

impl InitSocket for NoDelayAndKeepAlive {
    fn initialize_socket(&self, socket: &mut Socket) -> io::Result<()> {
        socket.set_nodelay(true)?;
        socket.set_keepalive(true)?;
        Ok(())
    }
}

pub trait ReconnectionPolicy: fmt::Debug + Send + Sync {
    fn reconnection_delays(&self) -> Box<dyn Iterator<Item = Duration> + Send + Sync>;
}

impl<F, I> ReconnectionPolicy for Closure<F>
where
    F: Send + Sync + Fn() -> I,
    I: Iterator<Item = Duration> + Send + Sync + 'static,
{
    fn reconnection_delays(&self) -> Box<dyn Iterator<Item = Duration> + Send + Sync> {
        Box::new(self.0())
    }
}

impl ReconnectionPolicy for Duration {
    fn reconnection_delays(&self) -> Box<dyn Iterator<Item = Duration> + Send + Sync> {
        Box::new(iter::repeat(*self))
    }
}

impl ReconnectionPolicy for Vec<Duration> {
    fn reconnection_delays(&self) -> Box<dyn Iterator<Item = Duration> + Send + Sync> {
        Box::new(self.clone().into_iter())
    }
}
