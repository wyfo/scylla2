use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use scylla2::{event::SessionEventHandler, topology::node::Node, SessionConfig};

mod utils;

// Prepared statements cache is cleared in case of table alteration
#[tokio::test]
async fn reprepare_statement() -> anyhow::Result<()> {
    const STATEMENT: &str = "INSERT INTO test (id) VALUES (?)";
    #[derive(Debug, Default, Clone)]
    struct EventHandler(Arc<AtomicUsize>);
    impl SessionEventHandler for EventHandler {
        fn reprepare_statement(&self, statement: &str, _id: &[u8], _node: &Arc<Node>) {
            assert_eq!(statement, STATEMENT);
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let event_handler = EventHandler::default();
    let session =
        utils::test_session!(SessionConfig::new().session_event_handler(event_handler.clone()));
    session
        .execute("CREATE TABLE test (id int PRIMARY KEY)", ())
        .await?;
    let stmt = session.prepare(STATEMENT, true, None).await?;
    assert_eq!(event_handler.0.load(Ordering::Relaxed), 0);
    // Clear prepared cache
    session.execute("ALTER TABLE test ADD a int", ()).await?;
    session.execute(&stmt, (0,)).await?;
    assert_eq!(event_handler.0.load(Ordering::Relaxed), 1);
    // Clear prepared cache
    session.execute("ALTER TABLE test ADD b int", ()).await?;
    session.execute((&stmt,), ((1,),)).await?;
    assert_eq!(event_handler.0.load(Ordering::Relaxed), 2);
    Ok(())
}
