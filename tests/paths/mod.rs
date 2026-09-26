//! The two ways a caller's SQL is answered: `Engine::query`, which runs the plan itself, and
//! `Engine::plan`, which hands the same shaped, charged plan to an executor (run here with
//! DataFusion's `collect`, as Moruna's `PlanSource` runs it partition by partition). A shape or
//! budget test runs through both, so there is one behaviour to hold and not two.
#![allow(dead_code)]

use std::collections::BTreeMap;

use datafusion::arrow::array::RecordBatch;
use peql::{Caller, Engine, Planned};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Path {
    Query,
    Plan,
}

pub const BOTH: [Path; 2] = [Path::Query, Path::Plan];

/// What either path returns that a shape or budget test looks at.
pub struct Answer {
    pub batches: Vec<RecordBatch>,
    pub rows: usize,
    pub budgets: BTreeMap<String, f64>,
    pub charges: BTreeMap<String, f64>,
    pub suppress_k: Option<u64>,
}

/// Execute every partition of a planned query.
pub async fn run(p: Planned) -> Answer {
    let batches = datafusion::physical_plan::collect(p.plan.clone(), p.ctx.task_ctx())
        .await
        .unwrap();
    Answer {
        rows: batches.iter().map(|b| b.num_rows()).sum(),
        batches,
        budgets: p.budgets,
        charges: p.charges,
        suppress_k: p.suppress_k,
    }
}

pub async fn answer(e: &Engine, sql: &str, caller: &Caller, path: Path) -> peql::Result<Answer> {
    match path {
        Path::Query => {
            let r = e.query(sql, caller).await?;
            Ok(Answer {
                rows: r.envelope.rows,
                batches: r.batches,
                budgets: r.envelope.budgets,
                charges: r.envelope.charges,
                suppress_k: r.envelope.suppress_k,
            })
        }
        Path::Plan => Ok(run(e.plan(sql, caller).await?).await),
    }
}
