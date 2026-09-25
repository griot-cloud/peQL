//! Results on the wire: Arrow IPC (the default), Parquet, or newline-delimited JSON.

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::json::LineDelimitedWriter;
use datafusion::parquet::arrow::ArrowWriter;

#[derive(Debug, thiserror::Error)]
pub enum FormatterError {
    #[error("Arrow IPC serialization failed: {0}")]
    ArrowIpc(String),
    #[error("Parquet serialization failed: {0}")]
    Parquet(String),
    #[error("JSON serialization failed: {0}")]
    Json(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ResultFormat {
    /// Arrow IPC file format.
    #[default]
    Arrow,
    Parquet,
    /// One JSON object per row.
    Json,
}

impl ResultFormat {
    /// From the wire enum: 1 Parquet, 2 JSON, anything else Arrow.
    pub fn from_proto_i32(v: i32) -> Self {
        match v {
            1 => ResultFormat::Parquet,
            2 => ResultFormat::Json,
            _ => ResultFormat::Arrow,
        }
    }
}

pub struct ResultFormatter;

impl ResultFormatter {
    /// Serialize batches in the requested format. No batches gives no bytes.
    pub fn format_results(
        batches: &[RecordBatch],
        format: ResultFormat,
    ) -> Result<Vec<u8>, FormatterError> {
        let Some(first) = batches.first() else {
            return Ok(Vec::new());
        };
        let schema = first.schema();
        let mut out = Vec::new();
        match format {
            ResultFormat::Arrow => {
                let mut w = FileWriter::try_new(&mut out, &schema)
                    .map_err(|e| FormatterError::ArrowIpc(e.to_string()))?;
                for b in batches {
                    w.write(b)
                        .map_err(|e| FormatterError::ArrowIpc(e.to_string()))?;
                }
                w.finish()
                    .map_err(|e| FormatterError::ArrowIpc(e.to_string()))?;
            }
            ResultFormat::Parquet => {
                let mut w = ArrowWriter::try_new(&mut out, schema, None)
                    .map_err(|e| FormatterError::Parquet(e.to_string()))?;
                for b in batches {
                    w.write(b)
                        .map_err(|e| FormatterError::Parquet(e.to_string()))?;
                }
                w.close()
                    .map_err(|e| FormatterError::Parquet(e.to_string()))?;
            }
            ResultFormat::Json => {
                let mut w = LineDelimitedWriter::new(&mut out);
                let refs: Vec<&RecordBatch> = batches.iter().collect();
                w.write_batches(&refs)
                    .map_err(|e| FormatterError::Json(e.to_string()))?;
                w.finish()
                    .map_err(|e| FormatterError::Json(e.to_string()))?;
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Date32Array, Decimal128Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("day", DataType::Date32, true),
                Field::new("amount", DataType::Decimal128(10, 2), true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Arc::new(Date32Array::from(vec![Some(19_000), None])),
                Arc::new(
                    Decimal128Array::from(vec![Some(12_345), None])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
            ],
        )
        .unwrap()
    }

    #[test]
    fn every_format_round_trips_or_reads() {
        let b = batch();
        let ipc =
            ResultFormatter::format_results(std::slice::from_ref(&b), ResultFormat::Arrow).unwrap();
        let back: Vec<RecordBatch> =
            datafusion::arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(ipc), None)
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
        assert_eq!(back, vec![b.clone()]);
        let pq = ResultFormatter::format_results(std::slice::from_ref(&b), ResultFormat::Parquet)
            .unwrap();
        assert_eq!(&pq[..4], b"PAR1");
        let json =
            String::from_utf8(ResultFormatter::format_results(&[b], ResultFormat::Json).unwrap())
                .unwrap();
        let lines: Vec<&str> = json.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(
            lines[0].contains("\"amount\":\"123.45\"") || lines[0].contains("\"amount\":123.45"),
            "{json}"
        );
        assert!(
            ResultFormatter::format_results(&[], ResultFormat::Json)
                .unwrap()
                .is_empty()
        );
    }
}
