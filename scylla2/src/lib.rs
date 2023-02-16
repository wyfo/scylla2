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
    use std::env;

    use crate::SessionConfig;

    #[tokio::test]
    async fn it_works() {
        let session = SessionConfig::new()
            .nodes([env::var("SCYLLA_URI").unwrap()])
            .connect()
            .await
            .unwrap();
        println!("connected");
        for node in session.topology().nodes() {
            println!("{node:?}");
        }
        println!(
            "{:?}",
            session
                .execute("SELECT key FROM system.local where key = 'local'", ())
                .await
                .unwrap()
                .rows::<(String,)>()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
        );
        let stmt = session
            .prepare("SELECT key FROM system.local where key = 'local'")
            .await
            .unwrap();
        println!("stmt = {stmt:?}");
        println!(
            "{:?}",
            session
                .execute(&stmt, ())
                .await
                .unwrap()
                .rows::<(String,)>()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
        );
    }
}
