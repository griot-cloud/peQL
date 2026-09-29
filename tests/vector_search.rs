//! Adversarial vector ranking under an actual parcel contract and registered Wasm function.
mod vector_support;
use datafusion::arrow::array::{Array, Float64Array};
use peql::{
    Caller, PeqlError,
    audit::{MemoryAudit, Outcome},
};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use vector_support::*;

#[tokio::test]
async fn row_filters_run_before_top_k_and_owner_results_do_not_leak_from_cache() {
    let dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(MemoryAudit::default());
    let engine = engine(dir.path(), audit.clone(), false).await;
    let query = sql("embedding", "[1.0, 0.0]", 1, "cosine");
    assert_eq!(
        ids(&engine.query(&query, &owner()).await.unwrap().batches),
        vec![1]
    );
    assert_eq!(
        ids(&engine.query(&query, &guest()).await.unwrap().batches),
        vec![2]
    );
    assert_eq!(
        ids(&engine.query(&query, &owner()).await.unwrap().batches),
        vec![1]
    );
    let plan = engine.plan(&query, &guest()).await.unwrap();
    let displayed = datafusion::physical_plan::displayable(plan.plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        displayed.contains("GateExec: contract=demo/vectors"),
        "{displayed}"
    );
    assert!(displayed.contains("Sort"), "{displayed}");
    assert!(
        displayed.find("Sort").unwrap() < displayed.find("GateExec").unwrap(),
        "ranking must be above the governed gate: {displayed}"
    );
    assert_eq!(
        engine
            .get("demo/vectors")
            .unwrap()
            .compilation
            .contract
            .functions
            .len(),
        1
    );
    let records = audit.records.lock().unwrap();
    assert!(matches!(records[0].outcome, Outcome::Answered));
    assert!(
        records[0]
            .contracts
            .iter()
            .any(|c| c.starts_with("demo/vectors@1#"))
    );
    assert_eq!(
        records[0].sql_sha256,
        hex::encode(Sha256::digest(query.as_bytes()))
    );
}
#[tokio::test]
async fn masked_embedding_is_excluded_before_ranking_not_after_top_k() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), Arc::new(MemoryAudit::default()), true).await;
    let query = sql("embedding", "[1.0, 0.0]", 1, "cosine");
    assert_eq!(
        ids(&engine.query(&query, &guest()).await.unwrap().batches),
        vec![3]
    );
    assert_eq!(
        ids(&engine.query(&query, &owner()).await.unwrap().batches),
        vec![1]
    );
}
#[tokio::test]
async fn exact_metrics_return_expected_distances_over_governed_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), Arc::new(MemoryAudit::default()), false).await;
    for (metric, expected) in [
        ("cosine", [0.0, 1.0 - 0.8 / 0.68_f64.sqrt(), 1.0]),
        ("euclidean", [0.0, 0.08_f64.sqrt(), 2.0_f64.sqrt()]),
        ("dot", [-1.0, -0.8, 0.0]),
    ] {
        let result = engine
            .query(&sql("embedding", "[1.0, 0.0]", 3, metric), &owner())
            .await
            .unwrap();
        assert_eq!(ids(&result.batches), vec![1, 2, 3], "{metric}");
        let scores = result.batches[0]
            .column(result.batches[0].schema().index_of("distance").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(scores.len(), expected.len());
        for (index, expected) in expected.into_iter().enumerate() {
            assert!(
                (scores.value(index) - expected).abs() < 1e-6,
                "{metric} row {index}"
            );
        }
    }
    let query = format!(
        "WITH nearest AS ({}) SELECT id FROM nearest",
        sql("embedding", "[1, 0]", 1, "cosine")
    );
    assert_eq!(
        ids(&engine.query(&query, &guest()).await.unwrap().batches),
        vec![2]
    );
}
#[tokio::test]
async fn malformed_queries_private_columns_and_policy_refusals_are_audited() {
    let dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(MemoryAudit::default());
    let engine = engine(dir.path(), audit.clone(), false).await;
    for query in [
        sql("embedding", "[]", 1, "cosine"),
        sql("embedding", "[1]", 1, "cosine"),
        sql("embedding", "[0, 0]", 1, "cosine"),
        sql("embedding", "[1, 0]", 0, "cosine"),
        sql("embedding", "[1, 0]", 1, "unknown"),
        sql("private_embedding", "[1, 0]", 1, "cosine"),
        sql("id", "[1, 0]", 1, "cosine"),
    ] {
        assert!(engine.query(&query, &owner()).await.is_err(), "{query}");
    }
    let query = sql("embedding", "[1, 0]", 1, "cosine");
    assert!(matches!(
        engine
            .query(&query, &Caller::new("spy", "unpublished", "analytics"))
            .await,
        Err(PeqlError::UnknownContract(_))
    ));
    assert!(matches!(
        engine
            .query(&query, &Caller::new("guest", "partner", "advertising"))
            .await,
        Err(PeqlError::Denied { .. })
    ));
    let records = audit.records.lock().unwrap();
    assert_eq!(records.len(), 9);
    assert!(
        records[..7]
            .iter()
            .all(|r| matches!(r.outcome, Outcome::Failed(_)))
    );
    assert!(
        records[7..]
            .iter()
            .all(|r| matches!(r.outcome, Outcome::Refused(_)))
    );
    assert_eq!(
        records[8].sql_sha256,
        hex::encode(Sha256::digest(query.as_bytes()))
    );
}
#[tokio::test]
async fn a_cte_cannot_replace_the_contract_target_and_remove_its_gate() {
    let dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(MemoryAudit::default());
    let engine = engine(dir.path(), audit.clone(), false).await;
    for cte in ["demo/vectors", "DEMO/VECTORS"] {
        let query = format!(
            "WITH \"{cte}\" AS (SELECT * FROM \"demo/vectors\") {}",
            sql("embedding", "[1, 0]", 1, "cosine")
        );
        assert!(engine.query(&query, &guest()).await.is_err());
    }
    assert_eq!(audit.records.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn multiple_searches_keep_their_query_vectors_separate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), Arc::new(MemoryAudit::default()), false).await;
    let query = "SELECT a.id AS left_id, b.id AS right_id FROM vector_search('demo/vectors', 'embedding', [1, 0], 1, 'cosine') a CROSS JOIN vector_search('demo/vectors', 'embedding', [0, 1], 1, 'euclidean') b";
    let result = engine.query(query, &guest()).await.unwrap();
    let batch = &result.batches[0];
    assert_eq!(batch.num_rows(), 1);
    for (column, expected) in [("left_id", 2), ("right_id", 3)] {
        let ids = batch
            .column(batch.schema().index_of(column).unwrap())
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), expected);
    }
}
#[tokio::test]
async fn an_exposed_distance_column_cannot_be_silently_replaced() {
    use datafusion::arrow::{
        array::RecordBatch,
        datatypes::{DataType, Field, Schema},
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = peql::Engine::in_memory(dir.path());
    let original = batch();
    let mut fields = original.schema().fields().to_vec();
    fields.push(Arc::new(Field::new("distance", DataType::Float64, false)));
    let schema = Arc::new(Schema::new(fields));
    let mut columns = original.columns().to_vec();
    columns.push(Arc::new(Float64Array::from(vec![4.0, 5.0, 6.0])));
    let data = RecordBatch::try_new(schema.clone(), columns).unwrap();
    engine.register_contract("contract: demo/collision\nversion: 1\nowner: demo\nbinding: {parquet: collision/}\nexpose: [{name: id, type: int64}, {name: embedding, type: 'fixed_size_list<float32, 2>'}, {name: distance, type: float64}]\nrules: []", &schema).unwrap();
    engine
        .write("demo/collision", vec![data], peql::WriteMode::Overwrite)
        .await
        .unwrap();
    let error = engine
        .query(
            "SELECT * FROM vector_search('demo/collision', 'embedding', [1, 0], 1, 'cosine')",
            &owner(),
        )
        .await
        .err()
        .expect("query must be refused");
    assert!(
        error
            .to_string()
            .contains("reserved result column distance"),
        "{error}"
    );
}

#[tokio::test]
async fn invalid_and_zero_norm_candidates_do_not_consume_cosine_top_k() {
    use datafusion::arrow::{
        array::{FixedSizeListArray, Int64Array, RecordBatch},
        datatypes::Float32Type,
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = peql::Engine::in_memory(dir.path());
    let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        [
            Some(vec![Some(0.0), Some(0.0)]),
            Some(vec![Some(f32::NAN), Some(1.0)]),
            Some(vec![Some(f32::INFINITY), Some(1.0)]),
            Some(vec![None, Some(1.0)]),
            None,
            Some(vec![Some(1.0), Some(0.0)]),
        ],
        2,
    );
    let data = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(vectors.clone()),
            Arc::new(vectors),
        ],
    )
    .unwrap();
    engine.register_contract("contract: demo/vectors\nversion: 1\nowner: demo\nbinding: {parquet: vectors/}\nexpose: [{name: id, type: int64}, {name: embedding, type: 'fixed_size_list<float32, 2>'}]\nrules: []", &schema()).unwrap();
    engine
        .write("demo/vectors", vec![data], peql::WriteMode::Overwrite)
        .await
        .unwrap();
    let cosine = engine
        .query(&sql("embedding", "[1, 0]", 6, "cosine"), &owner())
        .await
        .unwrap();
    assert_eq!(ids(&cosine.batches), vec![6]);
    let euclidean = engine
        .query(&sql("embedding", "[1, 0]", 6, "euclidean"), &owner())
        .await
        .unwrap();
    assert_eq!(
        ids(&euclidean.batches),
        vec![6, 1],
        "zero-norm is valid for euclidean but null/nonfinite candidates never rank"
    );
    for vector in ["[1e100, 0]", "['not-a-number', 0]"] {
        let error = engine
            .query(&sql("embedding", vector, 1, "cosine"), &owner())
            .await
            .err()
            .expect("query must be refused");
        assert!(error.to_string().contains("vector"), "{error}");
    }
}
