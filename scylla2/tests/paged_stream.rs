use std::pin::pin;

use futures::StreamExt;

mod utils;

#[tokio::test]
async fn paged_stream() -> anyhow::Result<()> {
    let session = utils::test_session!();
    session
        .execute("CREATE TABLE test (id int PRIMARY KEY)", ())
        .await?;
    for id in 0..10 {
        session
            .execute("INSERT INTO test (id) VALUES (?)", (id,))
            .await?;
    }
    let mut stream = pin!(session.execute_paged("SELECT id FROM test", (), 2));
    let mut id_sum = 0;
    while let Some(res) = stream.next().await.transpose()? {
        let rows = res.rows::<(i32,)>()?;
        assert!(rows.len() == 2 || (rows.len() == 0 && id_sum == 45));
        id_sum += rows.map(|row| row.unwrap().0).sum::<i32>();
    }
    assert_eq!(id_sum, 45);
    Ok(())
}
