#![allow(unused)]
use std::{cmp::min, collections::BTreeSet, sync::Mutex};

use scylla2::{Session, SessionConfig};

pub(crate) async fn test_session_internal(
    qualified_test_name: &str,
    config: impl Into<Option<SessionConfig>>,
) -> Session {
    tracing_subscriber::fmt::fmt()
        .with_test_writer()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .compact()
        .try_init()
        .ok();
    let test_name = qualified_test_name.split("::").nth(1).unwrap();
    let keyspace = format!("test_{}", &test_name[..min(test_name.len(), 43)]);
    {
        static KEYSPACES: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());
        let mut keyspaces = KEYSPACES.lock().unwrap();
        if !keyspaces.insert(keyspace.clone()) {
            panic!("{keyspace} is already used as keyspace name")
        }
    }
    let scylla_uri =
        std::env::var("SCYLLA_URI").expect("SCYLLA_URI must be set for integration tests");
    let session = config
        .into()
        .unwrap_or_default()
        .nodes([scylla_uri])
        .connect()
        .await
        .unwrap();
    session
        .execute(format!("DROP KEYSPACE IF EXISTS {keyspace}"), ())
        .await
        .unwrap();
    session.execute(format!("CREATE KEYSPACE {keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 1}}"), ()).await.unwrap();
    session.use_keyspace(keyspace).await.unwrap();
    session
}

macro_rules! test_session {
    () => {
        utils::test_session!(None)
    };
    ($builder:expr) => {
        utils::test_session_internal(stdext::function_name!(), $builder).await
    };
}

pub(crate) use test_session;
