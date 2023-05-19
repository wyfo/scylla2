use rand::Rng;
use scylla2::statement::Statement;

mod utils;

#[tokio::test]
async fn prepared_statement() -> anyhow::Result<()> {
    let session = utils::test_session!();
    session
        .execute("CREATE TABLE test (id int PRIMARY KEY, data text)", ())
        .await?;
    let id: i32 = rand::thread_rng().gen_range(0..100);
    session
        .execute("INSERT INTO test (id) VALUES (?)", (id,))
        .await?;
    let get_data = session
        .prepare("SELECT id, data FROM test WHERE id = ?", true, None)
        .await?;
    let token = session
        .execute("SELECT token(id) FROM test WHERE id = ?", (id,))
        .await?
        .rows::<(i64,)>()?
        .next()
        .unwrap()?
        .0;
    assert_eq!(get_data.partition(&(id,), None)?.unwrap().token().0, token);
    assert_eq!(
        session
            .execute(&get_data, (id,))
            .await?
            .rows::<(i32, Option<&str>)>()?
            .next()
            .unwrap()?,
        (id, None),
    );
    Ok(())
}
