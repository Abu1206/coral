//! Workspace-scoped persisted column statistics.

use std::collections::BTreeMap;

use coral_engine::{
    ColumnStatistics, SourceStatistics, StatisticValue, StatisticsObservation,
    StatisticsObservationScope, StatisticsProfile, TableStatistics,
};

use crate::bootstrap::AppError;
use crate::state::AppStateLayout;
use crate::storage::fs::{self as storage_fs, FileLock};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone)]
pub(crate) struct StatisticsStore {
    layout: AppStateLayout,
}

impl StatisticsStore {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    pub(crate) fn load_profile(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<StatisticsProfile, AppError> {
        let _lock = FileLock::shared(self.layout.state_lock())?;
        self.load_unlocked(workspace_name)
    }

    pub(crate) fn merge_observations(
        &self,
        workspace_name: &WorkspaceName,
        observations: &[StatisticsObservation],
    ) -> Result<(), AppError> {
        if !observations
            .iter()
            .any(|observation| observation.scope == StatisticsObservationScope::TableGlobal)
        {
            return Ok(());
        }

        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        let mut profile = self.load_unlocked(workspace_name)?;
        for observation in observations {
            merge_observation(&mut profile, observation);
        }
        self.save_unlocked(workspace_name, &profile)
    }

    fn load_unlocked(&self, workspace_name: &WorkspaceName) -> Result<StatisticsProfile, AppError> {
        let path = self.layout.statistics_profile_file(workspace_name);
        if !path.exists() {
            return Ok(StatisticsProfile::empty());
        }

        let raw = std::fs::read_to_string(&path)?;
        let profile: StatisticsProfile = serde_json::from_str(&raw)?;
        if profile.version != StatisticsProfile::empty().version {
            tracing::warn!(
                workspace = %workspace_name,
                version = profile.version,
                "ignoring unsupported statistics profile version"
            );
            return Ok(StatisticsProfile::empty());
        }
        Ok(profile)
    }

    fn save_unlocked(
        &self,
        workspace_name: &WorkspaceName,
        profile: &StatisticsProfile,
    ) -> Result<(), AppError> {
        let path = self.layout.statistics_profile_file(workspace_name);
        if let Some(parent) = path.parent() {
            storage_fs::ensure_dir(parent)?;
        }
        let raw = serde_json::to_vec_pretty(profile)?;
        storage_fs::write_atomic(&path, &raw)?;
        Ok(())
    }
}

fn merge_observation(profile: &mut StatisticsProfile, observation: &StatisticsObservation) {
    if observation.scope != StatisticsObservationScope::TableGlobal {
        return;
    }

    let source_stats = profile
        .sources
        .entry(observation.schema_name.clone())
        .or_insert_with(|| SourceStatistics {
            schema_name: observation.schema_name.clone(),
            source_version: observation.source_version.clone(),
            tables: BTreeMap::default(),
        });
    source_stats
        .source_version
        .clone_from(&observation.source_version);

    let replace_table = source_stats
        .tables
        .get(&observation.table_name)
        .is_none_or(|table| table.schema_signature != observation.schema_signature);
    if replace_table {
        source_stats.tables.insert(
            observation.table_name.clone(),
            TableStatistics {
                schema_name: observation.schema_name.clone(),
                table_name: observation.table_name.clone(),
                source_version: observation.source_version.clone(),
                schema_signature: observation.schema_signature.clone(),
                columns: BTreeMap::default(),
            },
        );
    }

    let table_stats = source_stats
        .tables
        .get_mut(&observation.table_name)
        .expect("table inserted above");
    table_stats
        .source_version
        .clone_from(&observation.source_version);

    for column in &observation.columns {
        let observed = ColumnStatistics {
            column_name: column.column_name.clone(),
            sample_count: column.sample_count,
            null_count: column.null_count.clone(),
            approx_distinct_count: column.approx_distinct_count.clone(),
            observed_at: Some(observation.observed_at.clone()),
        };
        table_stats
            .columns
            .entry(column.column_name.clone())
            .and_modify(|existing| merge_column(existing, &observed))
            .or_insert(observed);
    }
}

fn merge_column(existing: &mut ColumnStatistics, observed: &ColumnStatistics) {
    existing.sample_count = existing.sample_count.saturating_add(observed.sample_count);
    existing.null_count = merge_additive(existing.null_count.clone(), observed.null_count.clone());
    existing.approx_distinct_count = merge_max(
        existing.approx_distinct_count.clone(),
        observed.approx_distinct_count.clone(),
    );
    existing.observed_at =
        latest_observed_at(existing.observed_at.clone(), observed.observed_at.clone());
}

fn merge_additive(
    left: Option<StatisticValue<u64>>,
    right: Option<StatisticValue<u64>>,
) -> Option<StatisticValue<u64>> {
    match (left, right) {
        (Some(left), Some(right)) => Some(StatisticValue {
            value: left.value.saturating_add(right.value),
            precision: left.precision.weaker(right.precision),
        }),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn merge_max(
    left: Option<StatisticValue<u64>>,
    right: Option<StatisticValue<u64>>,
) -> Option<StatisticValue<u64>> {
    match (left, right) {
        (Some(left), Some(right)) => Some(StatisticValue {
            value: left.value.max(right.value),
            precision: left.precision.weaker(right.precision),
        }),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn latest_observed_at(left: Option<String>, right: Option<String>) -> Option<String> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use coral_engine::{
        ColumnSchemaSignature, ColumnStatisticsObservation, StatisticsObservation,
        StatisticsObservationScope, TableSchemaSignature,
    };
    use tempfile::tempdir;

    use super::StatisticsStore;
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;

    fn workspace() -> WorkspaceName {
        WorkspaceName::parse("default").expect("workspace")
    }

    fn store() -> (tempfile::TempDir, StatisticsStore) {
        let temp = tempdir().expect("tempdir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        (temp, StatisticsStore::new(layout))
    }

    fn signature(nullable: bool) -> TableSchemaSignature {
        TableSchemaSignature {
            columns: vec![ColumnSchemaSignature {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
                nullable,
                is_virtual: false,
                is_required_filter: false,
            }],
            required_filters: Vec::new(),
        }
    }

    fn observation(scope: StatisticsObservationScope) -> StatisticsObservation {
        StatisticsObservation {
            schema_name: "local".to_string(),
            table_name: "events".to_string(),
            source_version: Some("0.1.0".to_string()),
            schema_signature: signature(true),
            scope,
            observed_at: "2026-05-06T00:00:00Z".to_string(),
            columns: vec![ColumnStatisticsObservation {
                column_name: "name".to_string(),
                sample_count: 3,
                null_count: Some(coral_engine::StatisticValue {
                    value: 1,
                    precision: coral_engine::StatisticPrecision::ObservedSample,
                }),
                approx_distinct_count: Some(coral_engine::StatisticValue {
                    value: 2,
                    precision: coral_engine::StatisticPrecision::ObservedSample,
                }),
            }],
        }
    }

    fn event_table(profile: &coral_engine::StatisticsProfile) -> &coral_engine::TableStatistics {
        profile
            .sources
            .get("local")
            .expect("local source")
            .tables
            .get("events")
            .expect("events table")
    }

    fn name_column(profile: &coral_engine::StatisticsProfile) -> &coral_engine::ColumnStatistics {
        event_table(profile)
            .columns
            .get("name")
            .expect("name column")
    }

    #[test]
    fn missing_profile_loads_as_empty() {
        let (_temp, store) = store();
        let profile = store.load_profile(&workspace()).expect("profile");

        assert_eq!(profile.version, 1);
        assert!(profile.sources.is_empty());
    }

    #[test]
    fn profile_save_load_round_trips() {
        let (_temp, store) = store();
        let workspace = workspace();
        store
            .merge_observations(
                &workspace,
                &[observation(StatisticsObservationScope::TableGlobal)],
            )
            .expect("merge");

        let profile = store.load_profile(&workspace).expect("profile");

        assert_eq!(name_column(&profile).sample_count, 3);
    }

    #[test]
    fn non_table_global_observations_are_ignored() {
        let (_temp, store) = store();
        let workspace = workspace();
        store
            .merge_observations(
                &workspace,
                &[observation(StatisticsObservationScope::Filtered {
                    filter_columns: vec!["status".to_string()],
                })],
            )
            .expect("merge");

        let profile = store.load_profile(&workspace).expect("profile");

        assert!(profile.sources.is_empty());
    }

    #[test]
    fn matching_table_global_observations_merge_counts() {
        let (_temp, store) = store();
        let workspace = workspace();
        let observation = observation(StatisticsObservationScope::TableGlobal);
        store
            .merge_observations(&workspace, std::slice::from_ref(&observation))
            .expect("first merge");
        store
            .merge_observations(&workspace, &[observation])
            .expect("second merge");

        let profile = store.load_profile(&workspace).expect("profile");
        let column = name_column(&profile);

        assert_eq!(column.sample_count, 6);
        assert_eq!(column.null_count.as_ref().unwrap().value, 2);
        assert_eq!(column.approx_distinct_count.as_ref().unwrap().value, 2);
    }

    #[test]
    fn schema_signature_mismatch_replaces_old_table_stats() {
        let (_temp, store) = store();
        let workspace = workspace();
        let mut first = observation(StatisticsObservationScope::TableGlobal);
        first.schema_signature = signature(false);
        store
            .merge_observations(&workspace, &[first])
            .expect("first merge");
        store
            .merge_observations(
                &workspace,
                &[observation(StatisticsObservationScope::TableGlobal)],
            )
            .expect("second merge");

        let profile = store.load_profile(&workspace).expect("profile");
        let table = event_table(&profile);
        let column = table.columns.get("name").expect("name column");

        assert_eq!(column.sample_count, 3);
        assert_eq!(table.schema_signature, signature(true));
    }
}
