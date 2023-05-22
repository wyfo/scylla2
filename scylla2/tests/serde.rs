use scylla2::error::RowsError;
use scylla2_cql::Udt;

mod utils;

#[tokio::test]
async fn user_defined_type() -> anyhow::Result<()> {
    let session = utils::test_session!();
    session.execute("CREATE TYPE udt (a int)", ()).await?;
    session
        .execute("CREATE TABLE test (id int PRIMARY KEY, udt udt)", ())
        .await?;
    session
        .execute("INSERT INTO test (id, udt) VALUES (0, ?)", (Udt((42i32,)),))
        .await?;
    assert_eq!(
        session
            .execute("SELECT udt FROM test WHERE id = 0", ())
            .await?
            .rows::<(Udt<(i32,)>,)>()?
            .next()
            .unwrap()?,
        (Udt((42,)),)
    );
    session.execute("ALTER TYPE udt ADD b int", ()).await?;
    assert_eq!(
        session
            .execute("SELECT udt FROM test WHERE id = 0", ())
            .await?
            .rows::<(Udt<(i32,)>,)>()?
            .next()
            .unwrap()?,
        (Udt((42,)),)
    );
    assert_eq!(
        session
            .execute("SELECT udt FROM test WHERE id = 0", ())
            .await?
            .rows::<(Udt<(i32, Option<i32>)>,)>()?
            .next()
            .unwrap()?,
        (Udt((42, None)),)
    );
    session
        .execute(
            "INSERT INTO test (id, udt) VALUES (1, ?)",
            (Udt((1i32, 2i32)),),
        )
        .await?;
    assert_eq!(
        session
            .execute("SELECT udt FROM test WHERE id = 1", ())
            .await?
            .rows::<(Udt<(i32,)>,)>()?
            .next()
            .unwrap()?,
        (Udt((1,)),)
    );
    assert_eq!(
        session
            .execute("SELECT udt FROM test WHERE id = 1", ())
            .await?
            .rows::<(Udt<(i32, i32)>,)>()?
            .next()
            .unwrap()?,
        (Udt((1, 2)),)
    );
    Ok(())
}

#[tokio::test]
async fn row_type_error() -> anyhow::Result<()> {
    let session = utils::test_session!();
    let err = session
        .execute("SELECT key FROM system.local", ())
        .await?
        .rows::<(i32,)>()
        .unwrap_err();
    match err {
        RowsError::TypeError(err) => assert_eq!(err.to_string(), "Column 0: Unexpected type text"),
        err => panic!("Unexpected {err}"),
    }
    Ok(())
}
