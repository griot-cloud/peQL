//! Exact similarity ranking over the caller's governed contract view.
use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::array::{Array, FixedSizeListArray, Float32Array, Float64Array};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Query, Statement, TableFactor, Value, VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::error::{PeqlError, Result};

#[derive(Clone, Copy)]
enum Metric {
    Cosine,
    Euclidean,
    Dot,
}

pub(crate) struct Search {
    pub table: String,
    column: String,
    vector: Vec<f32>,
    metric: Metric,
    function: String,
}

fn invalid(message: impl Into<String>) -> PeqlError {
    PeqlError::Invalid(format!("vector_search: {}", message.into()))
}
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn text(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(i) => Ok(i.value.clone()),
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => Ok(s.clone()),
            _ => Err(invalid(
                "table, column and metric must be names or string literals",
            )),
        },
        _ => Err(invalid(
            "table, column and metric must be names or string literals",
        )),
    }
}
fn number(expr: &Expr) -> Result<f64> {
    let value = match expr {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => n.parse::<f64>().ok(),
            _ => None,
        },
        Expr::UnaryOp { op, expr } if op.to_string() == "-" => Some(-number(expr)?),
        _ => None,
    };
    value
        .filter(|v| v.is_finite())
        .ok_or_else(|| invalid("query vector must contain finite numbers"))
}

/// Expand a parsed table function into a subquery. Its only data input remains
/// a normal contract name, discovered and registered by Engine::prepare. Neither
/// the score nor top-k touches raw bindings or materializes a second table.
pub(crate) fn expand(sql: &str) -> Result<(String, Vec<Search>)> {
    struct Expand {
        searches: Vec<Search>,
        ctes: HashSet<String>,
    }
    impl VisitorMut for Expand {
        type Break = PeqlError;
        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
            if let Some(with) = &query.with {
                self.ctes.extend(
                    with.cte_tables
                        .iter()
                        .map(|cte| cte.alias.name.value.to_ascii_lowercase()),
                );
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<Self::Break> {
            let TableFactor::Table {
                name,
                args: Some(args),
                alias,
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
            } = factor
            else {
                return ControlFlow::Continue(());
            };
            if name.to_string().to_lowercase() != "vector_search" {
                return ControlFlow::Continue(());
            }
            let result = (|| -> Result<TableFactor> {
                if !with_hints.is_empty()
                    || version.is_some()
                    || *with_ordinality
                    || !partitions.is_empty()
                    || json_path.is_some()
                    || sample.is_some()
                    || !index_hints.is_empty()
                    || args.settings.is_some()
                {
                    return Err(invalid("table modifiers are not supported"));
                }
                let expressions = args
                    .args
                    .iter()
                    .map(|a| match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e),
                        _ => Err(invalid("five positional literal arguments are required")),
                    })
                    .collect::<Result<Vec<_>>>()?;
                if expressions.len() != 5 {
                    return Err(invalid(
                        "five arguments are required: table, column, query_vector, k, metric",
                    ));
                }
                let table = text(expressions[0])?;
                let column = text(expressions[1])?;
                let Expr::Array(array) = expressions[2] else {
                    return Err(invalid("query_vector must be an array literal"));
                };
                let vector = array
                    .elem
                    .iter()
                    .map(|e| {
                        let n = number(e)? as f32;
                        if n.is_finite() {
                            Ok(n)
                        } else {
                            Err(invalid("query vector values must fit float32"))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                if vector.is_empty() {
                    return Err(invalid("query vector dimension must be positive"));
                }
                let Expr::Value(v) = expressions[3] else {
                    return Err(invalid("k must be a positive integer"));
                };
                let k = match &v.value {
                    Value::Number(n, _) => n.parse::<usize>().ok(),
                    _ => None,
                }
                .filter(|k| *k > 0)
                .ok_or_else(|| invalid("k must be a positive integer"))?;
                let metric = match text(expressions[4])?.as_str() {
                    "cosine" => Metric::Cosine,
                    "euclidean" => Metric::Euclidean,
                    "dot" => Metric::Dot,
                    _ => return Err(invalid("metric must be cosine, euclidean or dot")),
                };
                if matches!(metric, Metric::Cosine) && vector.iter().all(|n| *n == 0.0) {
                    return Err(invalid("cosine query vector must have nonzero norm"));
                }
                let function = format!("__peql_vector_distance_{}", self.searches.len());
                let score = format!("{function}({})", quoted(&column));
                let query = format!(
                    "SELECT *, {score} AS distance FROM {} WHERE {score} IS NOT NULL ORDER BY distance ASC LIMIT {k}",
                    quoted(&table)
                );
                let Statement::Query(subquery) = Parser::parse_sql(&GenericDialect, &query)
                    .map_err(|e| invalid(e.to_string()))?
                    .remove(0)
                else {
                    unreachable!()
                };
                self.searches.push(Search {
                    table,
                    column,
                    vector,
                    metric,
                    function,
                });
                Ok(TableFactor::Derived {
                    lateral: false,
                    subquery,
                    alias: alias.clone(),
                    sample: None,
                })
            })();
            match result {
                Ok(replacement) => {
                    *factor = replacement;
                    ControlFlow::Continue(())
                }
                Err(e) => ControlFlow::Break(e),
            }
        }
    }
    let mut statements =
        Parser::parse_sql(&GenericDialect, sql).map_err(|e| invalid(e.to_string()))?;
    let mut expand = Expand {
        searches: vec![],
        ctes: HashSet::new(),
    };
    if let ControlFlow::Break(e) = statements.visit(&mut expand) {
        return Err(e);
    }
    if expand
        .searches
        .iter()
        .any(|search| expand.ctes.contains(&search.table.to_ascii_lowercase()))
    {
        return Err(invalid(
            "a CTE cannot shadow a vector_search contract target",
        ));
    }
    let rewritten = if expand.searches.is_empty() {
        sql.to_owned()
    } else {
        statements.remove(0).to_string()
    };
    Ok((rewritten, expand.searches))
}

impl Search {
    pub fn udf(&self, schema: &Schema) -> Result<ScalarUDF> {
        if schema.field_with_name("distance").is_ok() {
            return Err(invalid(
                "contract already exposes the reserved result column distance",
            ));
        }
        let field = schema
            .field_with_name(&self.column)
            .map_err(|_| invalid("embedding column is not exposed by the contract"))?;
        let DataType::FixedSizeList(element, dimension) = field.data_type() else {
            return Err(invalid(
                "embedding column must be fixed_size_list<float32, N>",
            ));
        };
        if *dimension <= 0 || element.data_type() != &DataType::Float32 {
            return Err(invalid(
                "embedding column must have positive dimension and float32 elements",
            ));
        }
        if *dimension as usize != self.vector.len() {
            return Err(invalid(format!(
                "query dimension {} does not match column dimension {dimension}",
                self.vector.len()
            )));
        }
        let query = self.vector.clone();
        let metric = self.metric;
        Ok(create_udf(
            &self.function,
            vec![field.data_type().clone()],
            DataType::Float64,
            Volatility::Immutable,
            Arc::new(move |args| {
                let arrays = ColumnarValue::values_to_arrays(args)?;
                let vectors = arrays[0]
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "invalid embedding column".into(),
                        )
                    })?;
                let values = (0..vectors.len())
                    .map(|row| {
                        if vectors.is_null(row) {
                            return None;
                        }
                        let array = vectors.value(row);
                        let vector = array.as_any().downcast_ref::<Float32Array>()?;
                        if vector.null_count() != 0
                            || vector.values().iter().any(|n| !n.is_finite())
                        {
                            return None;
                        }
                        let mut dot = 0.0;
                        let mut left = 0.0;
                        let mut right = 0.0;
                        let mut squared = 0.0;
                        for (a, b) in vector.values().iter().zip(&query) {
                            let a = *a as f64;
                            let b = *b as f64;
                            dot += a * b;
                            left += a * a;
                            right += b * b;
                            squared += (a - b) * (a - b);
                        }
                        match metric {
                            Metric::Euclidean => Some(squared.sqrt()),
                            Metric::Dot => Some(-dot),
                            Metric::Cosine if left > 0.0 => {
                                Some(1.0 - (dot / (left * right).sqrt()).clamp(-1.0, 1.0))
                            }
                            Metric::Cosine => None,
                        }
                    })
                    .collect::<Float64Array>();
                Ok(ColumnarValue::Array(Arc::new(values)))
            }),
        ))
    }
}
