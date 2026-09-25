//! Only queries reach the planner. The cases 0.3's DDL guard covered, plus the statements
//! a parser-based check refuses that a keyword scan could not see.

use peql::PeqlError;
use peql::guard::check_sql;

fn refused(sql: &str) -> String {
    match check_sql(sql) {
        Err(PeqlError::Refused { verb }) => verb,
        other => panic!("expected a refusal for {sql:?}, got {other:?}"),
    }
}

#[test]
fn statements_hidden_by_comments_are_refused() {
    assert_eq!(refused("/*hi*/ COPY foo FROM 'x'"), "COPY");
    assert_eq!(refused("/*hi*/COPY foo FROM 'x'"), "COPY");
    assert_eq!(
        refused("/* outer /* inner */ outer */ COPY foo FROM 'x'"),
        "COPY"
    );
    assert_eq!(
        refused("/* a /* b /* c */ b */ a */ INSTALL 'evil.so'"),
        "INSTALL"
    );
    assert_eq!(refused("-- evil\nCOPY foo FROM 'x'"), "COPY");
    assert_eq!(refused("-- innocent comment\nLOAD 'plugin.so'"), "LOAD");
    assert_eq!(
        refused("-- comment 1\n-- comment 2\nINSTALL 'evil.so'"),
        "INSTALL"
    );
}

#[test]
fn a_second_statement_is_refused() {
    for sql in [
        "SELECT 1; COPY foo FROM 'x'",
        "SELECT 1; SELECT 2; INSTALL 'evil.so'",
        "WITH cte AS (SELECT 1) SELECT * FROM cte; ATTACH DATABASE 'other.db' AS other",
        "/* comment */ SELECT 1; COPY tbl TO '/tmp/out.csv'",
        "SELECT 1; SELECT 2",
    ] {
        check_sql(sql).expect_err(sql);
    }
}

#[test]
fn everything_but_a_query_is_refused() {
    for sql in [
        "ATTACH 'foo.db' AS bar",
        "COPY tbl FROM '/data/file.csv'",
        "EXTENSION LOAD 'plugin'",
        "CREATE VIEW v AS SELECT 1 AS id",
        "CREATE EXTERNAL TABLE raw STORED AS PARQUET LOCATION 'orders/'",
        "INSERT INTO t VALUES (1)",
        "DELETE FROM t",
        "DROP TABLE t",
        "SET datafusion.execution.batch_size = 1",
        "EXPLAIN SELECT 1",
        "EXPLAIN ANALYZE SELECT 1",
    ] {
        check_sql(sql).expect_err(sql);
    }
}

#[test]
fn queries_are_allowed_whatever_their_strings_say() {
    for sql in [
        "SELECT id, name FROM users WHERE id = 1",
        "SELECT /* pick columns */ id FROM t",
        "SELECT 1;",
        "SELECT 'COPY' AS label FROM t",
        "SELECT '-- not a comment' AS msg FROM t",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r",
    ] {
        check_sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}
