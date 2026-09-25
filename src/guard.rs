//! Callers read through contracts and nothing else. Every statement is parsed; anything but
//! a single query is refused before planning, and the plan is checked again after it: no
//! DDL (`CREATE EXTERNAL TABLE` would reach raw files), no DML or `COPY`, no `SET`, no
//! `INSTALL`/`LOAD`/`ATTACH`, and no `EXPLAIN` (it would show bindings and rules).

use datafusion::common::tree_node::TreeNode;
use datafusion::execution::context::SQLOptions;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;

use crate::error::{PeqlError, Result};

/// Refuse anything that is not exactly one query. Comments and string literals cannot hide
/// a statement, because this reads the parsed statement, not the text.
pub fn check_sql(sql: &str) -> Result<()> {
    let statements = match DFParser::parse_sql(sql) {
        Ok(s) => s,
        Err(_) => {
            // Unparseable: name the first keyword if it is one we refuse, else report the parse error.
            let verb = first_keyword(sql);
            if REFUSED_VERBS.contains(&verb.as_str()) {
                return Err(PeqlError::Refused { verb });
            }
            return Err(PeqlError::Invalid(format!("cannot parse the query: {sql}")));
        }
    };
    if statements.len() != 1 {
        return Err(PeqlError::Refused {
            verb: "multiple statements".into(),
        });
    }
    match &statements[0] {
        Statement::Statement(s) if matches!(s.as_ref(), SqlStatement::Query(_)) => Ok(()),
        Statement::Statement(s) => Err(PeqlError::Refused {
            verb: statement_verb(s),
        }),
        Statement::Explain(_) => Err(PeqlError::Refused {
            verb: "EXPLAIN".into(),
        }),
        Statement::CreateExternalTable(_) => Err(PeqlError::Refused {
            verb: "CREATE EXTERNAL TABLE".into(),
        }),
        Statement::CopyTo(_) => Err(PeqlError::Refused {
            verb: "COPY".into(),
        }),
        _ => Err(PeqlError::Refused {
            verb: first_keyword(sql),
        }),
    }
}

/// The second check, on the logical plan: nothing but reads.
pub fn check_plan(plan: &LogicalPlan) -> Result<()> {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
        .verify_plan(plan)
        .map_err(|_| PeqlError::Refused {
            verb: "a statement that is not a query".into(),
        })?;
    let explains = plan
        .exists(|p| {
            Ok(matches!(
                p,
                LogicalPlan::Explain(_) | LogicalPlan::Analyze(_)
            ))
        })
        .unwrap_or(true);
    if explains {
        return Err(PeqlError::Refused {
            verb: "EXPLAIN".into(),
        });
    }
    Ok(())
}

const REFUSED_VERBS: &[&str] = &[
    "INSTALL", "LOAD", "ATTACH", "DETACH", "COPY", "CREATE", "DROP", "INSERT", "UPDATE", "DELETE",
    "ALTER", "SET", "EXPLAIN", "PRAGMA",
];

fn statement_verb(s: &SqlStatement) -> String {
    let text = s.to_string();
    let verb = first_keyword(&text);
    if verb.is_empty() {
        "statement".into()
    } else {
        verb
    }
}

/// The first word of the SQL once comments are removed, upper-cased.
fn first_keyword(sql: &str) -> String {
    let mut rest = sql.trim_start();
    loop {
        if let Some(r) = rest.strip_prefix("--") {
            rest = r
                .split_once('\n')
                .map(|(_, t)| t)
                .unwrap_or("")
                .trim_start();
        } else if let Some(r) = rest.strip_prefix("/*") {
            let mut depth = 1;
            let mut i = 0;
            let b = r.as_bytes();
            while i < b.len() && depth > 0 {
                if b[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if b[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            rest = r[i.min(r.len())..].trim_start();
        } else {
            break;
        }
    }
    rest.split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or("")
        .to_ascii_uppercase()
}
