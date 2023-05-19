use scylla2::{statement::Statement, Batch, PreparedStatement, Session};

mod utils;

async fn init_test(session: &Session) -> (PreparedStatement, PreparedStatement) {
    tokio::try_join!(
        session.execute("CREATE TABLE test1 (id int PRIMARY KEY)", ()),
        session.execute("CREATE TABLE test2 (id int PRIMARY KEY)", ())
    )
    .unwrap();
    tokio::try_join!(
        session.prepare("INSERT INTO test1 (id) VALUES (?)", true, None),
        session.prepare("INSERT INTO test2 (id) VALUES (?)", true, None),
    )
    .unwrap()
}

async fn test<B: Statement<((i32,), (i32,))>>(session: Session, batch: B) -> anyhow::Result<()> {
    let id: i32 = 42;
    let values = ((id,), (id,));
    session.execute(&batch, values).await?;
    let token = session
        .execute("SELECT token(id) FROM test1 WHERE id = ?", (id,))
        .await?
        .rows::<(i64,)>()?
        .next()
        .unwrap()?
        .0;
    assert_eq!(batch.partition(&values, None)?.unwrap().token().0, token,);
    Ok(())
}

#[tokio::test]
async fn raw_batch() {
    let session = utils::test_session!();
    let (stmt1, stmt2) = init_test(&session).await;
    test(session, (stmt1, stmt2)).await.unwrap();
}

#[tokio::test]
async fn batch() {
    let session = utils::test_session!();
    let (stmt1, stmt2) = init_test(&session).await;
    test(session, Batch::logged([stmt1, stmt2])).await.unwrap();
}
