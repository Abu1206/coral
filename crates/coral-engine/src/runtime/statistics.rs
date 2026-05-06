//! Runtime helpers for scan-level column statistics observations.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use chrono::{SecondsFormat, Utc};
use datafusion::arrow::array::{
    Array, BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, TimeUnit};

use crate::contracts::{
    ColumnSchemaSignature, ColumnStatisticsObservation, StatisticPrecision, StatisticValue,
    StatisticsObservation, StatisticsObservationScope, TableSchemaSignature,
};

/// Shared context passed into backend registration for runtime statistics.
#[derive(Clone, Default)]
pub(crate) struct RuntimeStatisticsContext {
    pub(crate) sink: StatisticsObservationSink,
}

impl RuntimeStatisticsContext {
    pub(crate) fn new(sink: StatisticsObservationSink) -> Self {
        Self { sink }
    }
}

/// Thread-safe sink for scan observations produced during one runtime execution.
#[derive(Debug, Clone, Default)]
pub(crate) struct StatisticsObservationSink {
    inner: Arc<Mutex<Vec<StatisticsObservation>>>,
}

impl StatisticsObservationSink {
    pub(crate) fn observe(&self, observation: StatisticsObservation) {
        match self.inner.lock() {
            Ok(mut observations) => observations.push(observation),
            Err(error) => tracing::warn!(
                detail = %error,
                "discarding statistics observation because sink lock is poisoned"
            ),
        }
    }

    pub(crate) fn drain(&self) -> Vec<StatisticsObservation> {
        match self.inner.lock() {
            Ok(mut observations) => std::mem::take(&mut *observations),
            Err(error) => {
                tracing::warn!(
                    detail = %error,
                    "discarding statistics observations because sink lock is poisoned"
                );
                Vec::new()
            }
        }
    }
}

/// Immutable scan metadata needed to turn Arrow batches into one observation.
#[derive(Debug, Clone)]
pub(crate) struct BatchStatisticsPlan {
    pub(crate) schema_name: String,
    pub(crate) table_name: String,
    pub(crate) source_version: Option<String>,
    pub(crate) schema_signature: TableSchemaSignature,
    pub(crate) scope: StatisticsObservationScope,
    pub(crate) precision: StatisticPrecision,
}

impl BatchStatisticsPlan {
    pub(crate) fn table_global(
        schema_name: impl Into<String>,
        table_name: impl Into<String>,
        source_version: Option<String>,
        schema_signature: TableSchemaSignature,
    ) -> Self {
        Self {
            schema_name: schema_name.into(),
            table_name: table_name.into(),
            source_version,
            schema_signature,
            scope: StatisticsObservationScope::TableGlobal,
            precision: StatisticPrecision::ObservedSample,
        }
    }

    pub(crate) fn with_scope(mut self, scope: StatisticsObservationScope) -> Self {
        self.scope = scope;
        self
    }
}

pub(crate) fn collect_batch_statistics(
    plan: &BatchStatisticsPlan,
    batches: &[RecordBatch],
) -> Option<StatisticsObservation> {
    if batches.is_empty() {
        return None;
    }

    let sample_count = batches
        .iter()
        .map(|batch| u64::try_from(batch.num_rows()).unwrap_or(u64::MAX))
        .fold(0_u64, u64::saturating_add);
    let mut columns = Vec::new();

    let schema = batches.first()?.schema();
    for field in schema.fields() {
        let Some(signature) = column_signature(plan, field.name()) else {
            continue;
        };
        if signature.is_virtual || signature.is_required_filter {
            continue;
        }

        let null_count = batches
            .iter()
            .filter_map(|batch| batch.column_by_name(field.name()))
            .map(|array| u64::try_from(array.null_count()).unwrap_or(u64::MAX))
            .fold(0_u64, u64::saturating_add);

        let approx_distinct_count =
            distinct_count_for_signature(signature, field.data_type(), batches).map(|value| {
                StatisticValue {
                    value,
                    precision: plan.precision,
                }
            });

        columns.push(ColumnStatisticsObservation {
            column_name: field.name().clone(),
            sample_count,
            null_count: Some(StatisticValue {
                value: null_count,
                precision: plan.precision,
            }),
            approx_distinct_count,
        });
    }

    if columns.is_empty() {
        return None;
    }

    Some(StatisticsObservation {
        schema_name: plan.schema_name.clone(),
        table_name: plan.table_name.clone(),
        source_version: plan.source_version.clone(),
        schema_signature: plan.schema_signature.clone(),
        scope: plan.scope.clone(),
        observed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        columns,
    })
}

fn column_signature<'a>(
    plan: &'a BatchStatisticsPlan,
    column_name: &str,
) -> Option<&'a ColumnSchemaSignature> {
    plan.schema_signature
        .columns
        .iter()
        .find(|column| column.name == column_name)
}

fn distinct_count_for_signature(
    signature: &ColumnSchemaSignature,
    arrow_data_type: &DataType,
    batches: &[RecordBatch],
) -> Option<u64> {
    if signature.data_type == "Json" {
        return None;
    }

    distinct_count(arrow_data_type, batches, &signature.name)
}

fn distinct_count(data_type: &DataType, batches: &[RecordBatch], column_name: &str) -> Option<u64> {
    match data_type {
        DataType::Utf8 => distinct_utf8(batches, column_name),
        DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8 => {
            distinct_cast_utf8(batches, column_name)
        }
        DataType::Int64 => distinct_int64::<Int64Array>(batches, column_name),
        DataType::Boolean => distinct_bool(batches, column_name),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            distinct_int64::<TimestampMicrosecondArray>(batches, column_name)
        }
        _ => None,
    }
}

fn distinct_cast_utf8(batches: &[RecordBatch], column_name: &str) -> Option<u64> {
    let mut values = HashSet::new();
    for batch in batches {
        let array = batch.column_by_name(column_name)?;
        let casted = cast(array, &DataType::Utf8).ok()?;
        let string_array = casted.as_any().downcast_ref::<StringArray>()?;
        for row in 0..string_array.len() {
            if string_array.is_valid(row) {
                values.insert(string_array.value(row).to_string());
            }
        }
    }
    Some(u64::try_from(values.len()).unwrap_or(u64::MAX))
}

fn distinct_utf8(batches: &[RecordBatch], column_name: &str) -> Option<u64> {
    let mut values = HashSet::new();
    for batch in batches {
        let array = batch
            .column_by_name(column_name)?
            .as_any()
            .downcast_ref::<StringArray>()?;
        for row in 0..array.len() {
            if array.is_valid(row) {
                values.insert(array.value(row).to_string());
            }
        }
    }
    Some(u64::try_from(values.len()).unwrap_or(u64::MAX))
}

fn distinct_int64<T>(batches: &[RecordBatch], column_name: &str) -> Option<u64>
where
    T: Array + 'static,
    for<'a> &'a T: Int64Values,
{
    let mut values = HashSet::new();
    for batch in batches {
        let array = batch
            .column_by_name(column_name)?
            .as_any()
            .downcast_ref::<T>()?;
        for row in 0..array.len() {
            if array.is_valid(row) {
                values.insert(array.int64_value(row));
            }
        }
    }
    Some(u64::try_from(values.len()).unwrap_or(u64::MAX))
}

trait Int64Values {
    fn int64_value(self, index: usize) -> i64;
}

impl Int64Values for &Int64Array {
    fn int64_value(self, index: usize) -> i64 {
        self.value(index)
    }
}

impl Int64Values for &TimestampMicrosecondArray {
    fn int64_value(self, index: usize) -> i64 {
        self.value(index)
    }
}

fn distinct_bool(batches: &[RecordBatch], column_name: &str) -> Option<u64> {
    let mut values = HashSet::new();
    for batch in batches {
        let array = batch
            .column_by_name(column_name)?
            .as_any()
            .downcast_ref::<BooleanArray>()?;
        for row in 0..array.len() {
            if array.is_valid(row) {
                values.insert(array.value(row));
            }
        }
    }
    Some(u64::try_from(values.len()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::{BatchStatisticsPlan, collect_batch_statistics};
    use crate::contracts::{
        ColumnSchemaSignature, StatisticsObservationScope, TableSchemaSignature,
    };

    fn signature() -> TableSchemaSignature {
        TableSchemaSignature {
            columns: vec![
                ColumnSchemaSignature {
                    name: "id".to_string(),
                    data_type: "Int64".to_string(),
                    nullable: false,
                    is_virtual: false,
                    is_required_filter: false,
                },
                ColumnSchemaSignature {
                    name: "name".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: true,
                    is_virtual: false,
                    is_required_filter: false,
                },
                ColumnSchemaSignature {
                    name: "score".to_string(),
                    data_type: "Float64".to_string(),
                    nullable: true,
                    is_virtual: false,
                    is_required_filter: false,
                },
                ColumnSchemaSignature {
                    name: "payload".to_string(),
                    data_type: "Json".to_string(),
                    nullable: true,
                    is_virtual: false,
                    is_required_filter: false,
                },
                ColumnSchemaSignature {
                    name: "filter".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: true,
                    is_virtual: true,
                    is_required_filter: true,
                },
            ],
            required_filters: vec!["filter".to_string()],
        }
    }

    #[test]
    fn collects_null_and_distinct_counts_for_supported_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("payload", DataType::Utf8, true),
            Field::new("filter", DataType::Utf8, true),
        ]));
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 3])),
                Arc::new(StringArray::from(vec![
                    Some("alpha"),
                    Some("beta"),
                    None,
                    Some("alpha"),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    Some(2.0),
                    None,
                    Some(2.0),
                ])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"kind":"alpha"}"#),
                    Some(r#"{"kind":"beta"}"#),
                    None,
                    Some(r#"{"kind":"alpha"}"#),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("x"),
                    Some("x"),
                    Some("x"),
                    Some("x"),
                ])),
            ],
        )
        .expect("batch");
        let plan =
            BatchStatisticsPlan::table_global("local", "events", Some("0.1.0".into()), signature());

        let observation = collect_batch_statistics(&plan, &[batch]).expect("observation");

        assert_eq!(observation.scope, StatisticsObservationScope::TableGlobal);
        assert_eq!(observation.columns.len(), 4);
        let by_name = observation
            .columns
            .iter()
            .map(|column| (column.column_name.as_str(), column))
            .collect::<std::collections::HashMap<_, _>>();
        let id = by_name.get("id").expect("id stats");
        let name = by_name.get("name").expect("name stats");
        let score = by_name.get("score").expect("score stats");
        let payload = by_name.get("payload").expect("payload stats");
        assert_eq!(id.approx_distinct_count.as_ref().unwrap().value, 3);
        assert_eq!(name.null_count.as_ref().unwrap().value, 1);
        assert_eq!(name.approx_distinct_count.as_ref().unwrap().value, 2);
        assert!(score.approx_distinct_count.is_none());
        assert_eq!(payload.null_count.as_ref().unwrap().value, 1);
        assert!(payload.approx_distinct_count.is_none());
        assert!(!by_name.contains_key("filter"));
    }
}
