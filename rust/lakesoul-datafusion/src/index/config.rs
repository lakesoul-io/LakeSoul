// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Generic index configuration parsing.
//!
//! A table property `{kind}_index_columns` holds a JSON array of entries;
//! every entry shares the column name and the management knobs
//! ([`IndexManagementConfig`]) and carries the kind-specific parameters in
//! [`IndexTableConfig::params`].  The parameters are flattened, so the
//! property keeps the flat shape both kinds have always used.

use lakesoul_common::IndexKind;
use rootcause::bail;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Result;

/// Default index rebuild strategy.
pub fn default_rebuild_mode() -> String {
    "auto".to_string()
}

/// Default auto-rebuild drift ratio.
pub fn default_max_delta_ratio() -> f32 {
    1.0
}

/// Default for deleting superseded index files after commits.
pub fn default_gc_enabled() -> bool {
    true
}

/// Default grace period protecting readers of older commits.
pub fn default_gc_grace_seconds() -> u64 {
    3600
}

/// Default number of generations kept before deletion.
pub fn default_gc_keep_generations() -> usize {
    1
}

/// Management knobs shared by every index kind.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexManagementConfig {
    /// Index rebuild strategy: `"auto"` (default) rebuilds a shard from
    /// scratch when its rebuild trigger fires; `"none"` only ever appends
    /// deltas.
    #[serde(default = "default_rebuild_mode")]
    pub rebuild_mode: String,
    /// Auto-rebuild trigger; its exact meaning is kind-specific (the vector
    /// index uses the per-cluster delta/base ratio).
    #[serde(default = "default_max_delta_ratio")]
    pub max_delta_ratio: f32,
    /// Delete superseded index files after commits (best effort).
    #[serde(default = "default_gc_enabled")]
    pub gc_enabled: bool,
    /// Files superseded for at least this long are eligible for deletion;
    /// protects readers that resolved an older commit.
    #[serde(default = "default_gc_grace_seconds")]
    pub gc_grace_seconds: u64,
    /// Generations to keep (including the current one) before deleting.
    #[serde(default = "default_gc_keep_generations")]
    pub gc_keep_generations: usize,
}

impl Default for IndexManagementConfig {
    fn default() -> Self {
        Self {
            rebuild_mode: default_rebuild_mode(),
            max_delta_ratio: default_max_delta_ratio(),
            gc_enabled: default_gc_enabled(),
            gc_grace_seconds: default_gc_grace_seconds(),
            gc_keep_generations: default_gc_keep_generations(),
        }
    }
}

impl IndexManagementConfig {
    /// Validate the management knobs of one column.
    pub fn validate(&self, kind: IndexKind, column: &str) -> Result<()> {
        let rebuild_mode = self.rebuild_mode.to_lowercase();
        if rebuild_mode != "auto" && rebuild_mode != "none" {
            bail!(
                "{kind} index column '{column}' rebuild_mode must be \"auto\" or \"none\", \
                 got {:?}",
                self.rebuild_mode
            );
        }
        if self.max_delta_ratio <= 0.0 || self.max_delta_ratio.is_nan() {
            bail!(
                "{kind} index column '{column}' max_delta_ratio must be > 0, got {}",
                self.max_delta_ratio
            );
        }
        Ok(())
    }
}

/// One entry of a `{kind}_index_columns` table property.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexTableConfig<S> {
    /// Indexed column name (must exist in the table schema).
    pub column: String,
    /// Kind-specific parameters (dim/nlist/... or tokenizer/...).
    #[serde(flatten)]
    pub params: S,
    /// Management knobs shared by every kind.
    #[serde(flatten)]
    pub management: IndexManagementConfig,
}

/// Parse a raw `{kind}_index_columns` value into index configurations.
///
/// The value is normally a JSON string containing a JSON array; a raw JSON
/// array is accepted as well.  Empty or missing values yield an empty list.
pub fn parse_index_columns<C: DeserializeOwned>(raw: Option<&str>) -> Result<Vec<C>> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    let raw = raw.trim();
    if raw.is_empty() || raw == "[]" {
        return Ok(Vec::new());
    }
    let value: serde_json::Value = serde_json::from_str(raw)?;
    let array = match value {
        serde_json::Value::String(s) => {
            let inner = s.trim();
            if inner.is_empty() {
                return Ok(Vec::new());
            }
            serde_json::from_str::<serde_json::Value>(inner)?
        }
        value => value,
    };
    Ok(serde_json::from_value(array)?)
}

/// Extract index configurations from a table's raw properties JSON (the
/// `TableInfo.properties` column).
pub fn parse_index_from_table_properties<C: DeserializeOwned>(
    properties_json: &str,
    property_key: &str,
) -> Result<Vec<C>> {
    let properties: serde_json::Value = serde_json::from_str(properties_json)?;
    match properties.get(property_key) {
        Some(serde_json::Value::String(s)) => parse_index_columns(Some(s)),
        Some(value) => Ok(serde_json::from_value(value.clone())?),
        None => Ok(Vec::new()),
    }
}

/// Serialize configurations into a `{kind}_index_columns` property value.
pub fn index_columns_to_json<C: Serialize>(configs: &[C]) -> String {
    serde_json::to_string(configs).unwrap_or_else(|_| "[]".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct TestParams {
        dim: usize,
    }

    #[test]
    fn parses_flattened_params_and_management() {
        let configs: Vec<IndexTableConfig<TestParams>> = parse_index_columns(Some(
            r#"[{"column":"vec","dim":8,"rebuild_mode":"none","max_delta_ratio":0.25}]"#,
        ))
        .unwrap();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].column, "vec");
        assert_eq!(configs[0].params.dim, 8);
        assert_eq!(configs[0].management.rebuild_mode, "none");
        assert_eq!(configs[0].management.max_delta_ratio, 0.25);
        assert_eq!(
            configs[0].management.gc_grace_seconds, 3600,
            "defaults apply"
        );
    }

    #[test]
    fn management_defaults_apply_when_absent() {
        let configs: Vec<IndexTableConfig<TestParams>> =
            parse_index_columns(Some(r#"[{"column":"vec","dim":8}]"#)).unwrap();
        assert_eq!(configs[0].management.rebuild_mode, "auto");
        assert!(configs[0].management.gc_enabled);
    }

    #[test]
    fn roundtrips_through_the_property_value() {
        let configs = vec![IndexTableConfig {
            column: "vec".to_string(),
            params: TestParams { dim: 4 },
            management: IndexManagementConfig {
                rebuild_mode: "none".to_string(),
                ..Default::default()
            },
        }];
        let json = index_columns_to_json(&configs);
        let parsed: Vec<IndexTableConfig<TestParams>> =
            parse_index_columns(Some(&json)).unwrap();
        assert_eq!(parsed[0].column, "vec");
        assert_eq!(parsed[0].params.dim, 4);
        assert_eq!(parsed[0].management.rebuild_mode, "none");
    }

    #[test]
    fn validates_rebuild_options() {
        let management = IndexManagementConfig {
            rebuild_mode: "sometimes".to_string(),
            ..Default::default()
        };
        assert!(management.validate(IndexKind::Vector, "vec").is_err());
        let management = IndexManagementConfig {
            max_delta_ratio: 0.0,
            ..Default::default()
        };
        assert!(management.validate(IndexKind::Vector, "vec").is_err());
        assert!(
            IndexManagementConfig::default()
                .validate(IndexKind::Vector, "vec")
                .is_ok()
        );
    }
}
