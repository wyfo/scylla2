use scylla2::error::RowsError;

mod utils;

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
