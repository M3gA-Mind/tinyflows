use super::*;
use tinystoragedrivers_sqlite::rusqlite::Connection;

fn build(path: &Path) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(
        "CREATE TABLE things (id TEXT PRIMARY KEY, label TEXT);
         INSERT INTO things VALUES ('a', 'first');",
    )
    .unwrap();
}

#[tokio::test]
async fn a_missing_file_is_no_database_rather_than_an_error() {
    let dir = tempfile::tempdir().unwrap();
    assert!(LegacyDb::open(&dir.path().join("nope.db")).unwrap().is_none());
}

#[tokio::test]
async fn tables_and_columns_are_listed_from_a_copy() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");
    build(&path);
    let db = LegacyDb::open(&path).unwrap().unwrap();
    assert!(db.tables().await.unwrap().contains("things"));
    let columns = db.columns("things").await.unwrap();
    assert!(columns.contains("id") && columns.contains("label"));
    assert!(db.columns("absent").await.unwrap().is_empty());
}

#[tokio::test]
async fn reading_leaves_the_original_byte_for_byte_alone() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");
    build(&path);
    let before = std::fs::read(&path).unwrap();
    let listing_before: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();

    let db = LegacyDb::open(&path).unwrap().unwrap();
    let labels: Vec<String> = db
        .read("labels", |conn| {
            let mut statement = conn.prepare("SELECT label FROM things")?;
            statement
                .query_map([], |row| row.get(0))?
                .collect::<Result<_, _>>()
        })
        .await
        .unwrap();
    drop(db);

    assert_eq!(labels, ["first"]);
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let listing_after: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(listing_after, listing_before, "no -wal or -shm appeared");
}

#[tokio::test]
async fn rows_that_only_live_in_the_wal_are_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");
    build(&path);
    // Keep a connection open so the WAL is not folded back into the main file.
    let writer = Connection::open(&path).unwrap();
    writer.pragma_update(None, "journal_mode", "WAL").unwrap();
    writer.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    writer
        .execute("INSERT INTO things VALUES ('b', 'in the wal')", [])
        .unwrap();
    assert!(dir.path().join("legacy.db-wal").is_file());

    let db = LegacyDb::open(&path).unwrap().unwrap();
    let count: i64 = db
        .read("count", |conn| {
            conn.query_row("SELECT COUNT(*) FROM things", [], |row| row.get(0))
        })
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn a_file_that_is_not_a_database_is_an_error_naming_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("junk.db");
    std::fs::write(&path, b"this is not sqlite at all, just text, long enough to be a header.....").unwrap();
    let outcome = match LegacyDb::open(&path) {
        Err(error) => error,
        Ok(Some(db)) => db.tables().await.unwrap_err(),
        Ok(None) => panic!("the file exists"),
    };
    assert!(format!("{outcome:#}").contains("junk.db") || format!("{outcome:#}").contains("legacy database"));
}

#[test]
fn a_missing_column_reads_as_its_default() {
    let have: HashSet<String> = ["id".to_string()].into();
    assert_eq!(column_or(&have, "id", "0"), "id");
    assert_eq!(column_or(&have, "extra", "NULL"), "NULL AS extra");
}
