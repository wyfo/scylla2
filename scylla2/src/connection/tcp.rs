use std::{
    io,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use scylla2_cql::{
    error::DatabaseErrorKind, protocol::get_supported, response::supported::Supported,
    ProtocolVersion,
};
use socket2::{Domain, Protocol, Socket, Type};
use strum::IntoEnumIterator;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpSocket, TcpStream},
};

use crate::{
    connection::config::InitSocket, error::ConnectionError, topology::sharding::ShardInfo,
};

pub(crate) enum TcpConnection {
    Tcp(TcpStream),
    #[cfg(feature = "ssl")]
    Ssl(tokio_openssl::SslStream<TcpStream>),
}

impl TcpConnection {
    async fn new(
        addr: SocketAddr,
        shard: Option<ShardInfo>, // shard != None assumes that node port is shard-aware
        init_socket: &dyn InitSocket,
        #[cfg(feature = "ssl")] ssl_context: Option<&openssl::ssl::SslContext>,
    ) -> io::Result<Self> {
        let mut socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
        init_socket.initialize_socket(&mut socket)?;
        socket.set_nonblocking(true)?;
        let socket = TcpSocket::from_std_stream(socket.into());
        if let Some(shard) = shard {
            let ip_zero = match addr.ip() {
                IpAddr::V4(_) => IpAddr::from([0; 4]),
                IpAddr::V6(_) => IpAddr::from([0; 16]),
            };
            let cannot_use = |error: &io::Error| {
                matches!(
                    error.kind(),
                    io::ErrorKind::AddrInUse | io::ErrorKind::PermissionDenied
                )
            };
            for port in shard.shard_aware_source_port() {
                match socket.bind(SocketAddr::new(ip_zero, port)) {
                    Err(err) if cannot_use(&err) => {}
                    _ => break,
                }
            }
        }
        let stream = socket.connect(addr).await?;
        #[cfg(feature = "ssl")]
        if let Some(ctx) = ssl_context {
            let ssl = openssl::ssl::Ssl::new(ctx)?;
            let mut ssl_stream = tokio_openssl::SslStream::new(ssl, stream)?;
            Pin::new(&mut ssl_stream)
                .connect()
                .await
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
            return Ok(Self::Ssl(ssl_stream));
        }
        Ok(Self::Tcp(stream))
    }

    pub(crate) async fn open(
        address: SocketAddr,
        shard: Option<ShardInfo>, // shard != None assumes that node port is shard-aware
        init_socket: &dyn InitSocket,
        #[cfg(feature = "ssl")] ssl_context: Option<&openssl::ssl::SslContext>,
        timeout: Duration,
        version: ProtocolVersion,
    ) -> Result<(Self, Supported), ConnectionError> {
        let conn_fut = Self::new(
            address,
            shard,
            init_socket,
            #[cfg(feature = "ssl")]
            ssl_context,
        );
        let mut conn = tokio::time::timeout(timeout, conn_fut).await??;
        let supported = get_supported(&mut conn, version).await?;
        Ok((conn, supported))
    }

    pub(crate) async fn open_with_minimal_version(
        address: SocketAddr,
        shard: Option<ShardInfo>, // shard != None assumes that node port is shard-aware
        init_socket: &dyn InitSocket,
        #[cfg(feature = "ssl")] ssl_context: Option<&openssl::ssl::SslContext>,
        timeout: Duration,
        minimal_version: Option<ProtocolVersion>,
    ) -> Result<(Self, ProtocolVersion, Supported), ConnectionError> {
        let minimal_version = minimal_version
            .or_else(|| ProtocolVersion::iter().next())
            .unwrap();
        let mut error = None;
        for version in ProtocolVersion::iter()
            .rev()
            .take_while(|v| *v >= minimal_version)
        {
            match Self::open(
                address,
                shard,
                init_socket,
                #[cfg(feature = "ssl")]
                ssl_context,
                timeout,
                version,
            )
            .await
            {
                Ok((conn, supported)) => return Ok((conn, version, supported)),
                Err(ConnectionError::Database(err))
                    if err.kind == DatabaseErrorKind::ProtocolError =>
                {
                    error = Some(err);
                }
                Err(err) => return Err(err),
            }
        }
        Err(error.unwrap().into())
    }

    pub(crate) fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Self::Tcp(stream) => stream.peer_addr(),
            #[cfg(feature = "ssl")]
            Self::Ssl(stream) => stream.get_ref().peer_addr(),
        }
    }
}

impl AsyncRead for TcpConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "ssl")]
            Self::Ssl(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TcpConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "ssl")]
            Self::Ssl(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "ssl")]
            Self::Ssl(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "ssl")]
            Self::Ssl(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            #[cfg(feature = "ssl")]
            Self::Ssl(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Tcp(stream) => stream.is_write_vectored(),
            #[cfg(feature = "ssl")]
            Self::Ssl(stream) => stream.is_write_vectored(),
        }
    }
}
