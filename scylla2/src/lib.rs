#![warn(missing_debug_implementations)]

pub mod auth;
pub mod connection;
pub mod error;
pub mod execution;
pub mod session;
pub mod statement;
pub mod topology;
mod utils;

pub use scylla2_cql as cql;
pub use scylla2_cql::event::{Event as DatabaseEvent, EventType as DatabaseEventType};

pub use crate::{
    session::{config::SessionConfig, event::SessionEvent, Session},
    statement::{
        batch::{Batch, BatchN},
        config::StatementConfig,
        prepared::PreparedStatement,
        query::Query,
    },
};

#[cfg(test)]
mod test {
    use crate::SessionConfig;

    #[tokio::test]
    async fn it_works() {
        println!("{:?}", &[][0..0] as &[u8]);
        let session = SessionConfig::new()
            .nodes(["10.69.0.2:19042"])
            .connect()
            .await
            .unwrap();
        println!("connected");
        for node in session.topology().nodes() {
            println!("{}", node.address());
        }
        println!("{:?}", session.execute("", ()).await);
    }
}
