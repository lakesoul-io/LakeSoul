// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! 向量索引配置类型。

use serde::Deserialize;

use crate::rabitq::{Metric, RotatorType};
use rootcause::{bail, report};

type Result<T> = std::result::Result<T, rootcause::Report>;

/// 向量索引配置。
///
/// 通过表属性 `vector_index_columns`（JSON）解析。支持单列（对象）或多列（数组）：
/// ```json
/// [{"column": "vec", "dim": 768, "nlist": 256, "total_bits": 7, "metric": "L2"}]
/// ```
/// 字段：
/// - `column`（必填）：向量列名
/// - `dim`（必填）：向量维度
/// - `nlist`（默认 256）：IVF 聚类数
/// - `total_bits`（默认 7）：RaBitQ 总位数，1-16
/// - `metric`（默认 `"L2"`）：`"L2"` 或 `"IP"`
/// - `rotator_type`（默认 `"FhtKac"`）：`"FhtKac"` 或 `"Matrix"`
/// - `seed`（默认 42）：随机种子
/// - `use_faster_config`（默认 true）：快速量化
#[derive(Debug, Clone)]
pub struct VectorIndexConfig {
    /// 向量列名（在 Arrow Schema 中的字段名）
    pub column_name: String,
    /// 向量维度
    pub dim: usize,
    /// IVF 聚类数
    pub nlist: usize,
    /// RaBitQ 总位数，1-16
    pub total_bits: usize,
    /// 距离度量
    pub metric: Metric,
    /// 旋转器类型
    pub rotator_type: RotatorType,
    /// 随机种子
    pub seed: u64,
    /// 是否使用快速量化配置（速度快 100-500x，精度损失 <1%）
    pub use_faster_config: bool,
}

impl Default for VectorIndexConfig {
    fn default() -> Self {
        Self {
            column_name: String::new(),
            dim: 0,
            nlist: 256,
            total_bits: 7,
            metric: Metric::L2,
            rotator_type: RotatorType::FhtKacRotator,
            seed: 42,
            use_faster_config: true,
        }
    }
}

/// 用于反序列化 `vector_index_columns` JSON 条目的中间类型。
#[derive(Debug, Deserialize)]
struct JsonEntry {
    column: String,
    dim: usize,
    #[serde(default = "default_nlist")]
    nlist: usize,
    #[serde(default = "default_total_bits")]
    total_bits: usize,
    #[serde(default = "default_metric")]
    metric: String,
    #[serde(default = "default_rotator")]
    rotator_type: String,
    #[serde(default = "default_seed")]
    seed: u64,
    #[serde(default = "default_faster")]
    use_faster_config: bool,
}

fn default_nlist() -> usize {
    256
}
fn default_total_bits() -> usize {
    7
}
fn default_metric() -> String {
    "L2".to_string()
}
fn default_rotator() -> String {
    "FhtKac".to_string()
}
fn default_seed() -> u64 {
    42
}
fn default_faster() -> bool {
    true
}

impl JsonEntry {
    fn into_config(self) -> Result<VectorIndexConfig> {
        if self.dim == 0 {
            bail!(
                "invalid vector index column '{}': dim must be > 0",
                self.column
            );
        }
        if !(1..=16).contains(&self.total_bits) {
            bail!(
                "invalid total_bits {} for column '{}': must be in 1..=16",
                self.total_bits,
                self.column
            );
        }
        let metric = match self.metric.to_uppercase().as_str() {
            "L2" => Metric::L2,
            "IP" | "INNERPRODUCT" => Metric::InnerProduct,
            other => bail!(
                "unknown metric '{}' for column '{}', expected 'L2' or 'IP'",
                other,
                self.column
            ),
        };
        let rotator_type = match self.rotator_type.to_uppercase().as_str() {
            "FHTKAC" | "FHT" => RotatorType::FhtKacRotator,
            "MATRIX" => RotatorType::MatrixRotator,
            other => bail!(
                "unknown rotator_type '{}' for column '{}', expected 'FhtKac' or 'Matrix'",
                other,
                self.column
            ),
        };
        Ok(VectorIndexConfig {
            column_name: self.column,
            dim: self.dim,
            nlist: self.nlist,
            total_bits: self.total_bits,
            metric,
            rotator_type,
            seed: self.seed,
            use_faster_config: self.use_faster_config,
        })
    }
}

impl VectorIndexConfig {
    /// 从 `vector_index_columns` 属性的 JSON 值解析多个向量列配置。
    ///
    /// 输入可以是单个对象或对象数组（见类型文档）。空字符串返回空列表。
    pub fn parse_json(value: &str) -> Result<Vec<Self>> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }
        let entries: Vec<JsonEntry> = if trimmed.starts_with('[') {
            serde_json::from_str(trimmed)
                .map_err(|e| report!("invalid vector_index_columns JSON: {}", e))?
        } else {
            let single: JsonEntry = serde_json::from_str(trimmed)
                .map_err(|e| report!("invalid vector_index_columns JSON: {}", e))?;
            vec![single]
        };
        entries.into_iter().map(JsonEntry::into_config).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_json_single_minimal() {
        let configs =
            VectorIndexConfig::parse_json(r#"{"column":"emb","dim":768}"#).unwrap();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].column_name, "emb");
        assert_eq!(configs[0].dim, 768);
        assert_eq!(configs[0].nlist, 256);
        assert_eq!(configs[0].total_bits, 7);
        assert!(matches!(configs[0].metric, Metric::L2));
        assert!(matches!(
            configs[0].rotator_type,
            RotatorType::FhtKacRotator
        ));
        assert_eq!(configs[0].seed, 42);
        assert!(configs[0].use_faster_config);
    }

    #[test]
    fn test_parse_json_multiple_full() {
        let input = r#"[
            {"column":"emb","dim":768,"nlist":512,"total_bits":7,"metric":"L2","rotator_type":"FhtKac","seed":42,"use_faster_config":true},
            {"column":"emb2","dim":512,"nlist":128,"total_bits":8,"metric":"IP","rotator_type":"Matrix","seed":123,"use_faster_config":false}
        ]"#;
        let configs = VectorIndexConfig::parse_json(input).unwrap();
        assert_eq!(configs.len(), 2);

        assert_eq!(configs[0].column_name, "emb");
        assert_eq!(configs[0].dim, 768);
        assert_eq!(configs[0].nlist, 512);
        assert!(matches!(configs[0].metric, Metric::L2));
        assert!(matches!(
            configs[0].rotator_type,
            RotatorType::FhtKacRotator
        ));
        assert!(configs[0].use_faster_config);

        assert_eq!(configs[1].column_name, "emb2");
        assert_eq!(configs[1].dim, 512);
        assert_eq!(configs[1].nlist, 128);
        assert!(matches!(configs[1].metric, Metric::InnerProduct));
        assert!(matches!(
            configs[1].rotator_type,
            RotatorType::MatrixRotator
        ));
        assert!(!configs[1].use_faster_config);
    }

    #[test]
    fn test_parse_json_case_insensitive_metric() {
        let configs =
            VectorIndexConfig::parse_json(r#"{"column":"emb","dim":8,"metric":"ip"}"#)
                .unwrap();
        assert!(matches!(configs[0].metric, Metric::InnerProduct));
    }

    #[test]
    fn test_parse_json_empty() {
        let configs = VectorIndexConfig::parse_json("").unwrap();
        assert!(configs.is_empty());
        let configs = VectorIndexConfig::parse_json("  ").unwrap();
        assert!(configs.is_empty());
    }

    #[test]
    fn test_parse_json_missing_dim() {
        let result = VectorIndexConfig::parse_json(r#"{"column":"emb"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_json_zero_dim() {
        let result = VectorIndexConfig::parse_json(r#"{"column":"emb","dim":0}"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_json_bad_metric() {
        let result = VectorIndexConfig::parse_json(
            r#"{"column":"emb","dim":8,"metric":"cosine"}"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_json_bad_total_bits() {
        let result =
            VectorIndexConfig::parse_json(r#"{"column":"emb","dim":8,"total_bits":32}"#);
        assert!(result.is_err());
    }
}
