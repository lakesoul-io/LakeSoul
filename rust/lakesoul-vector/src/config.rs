// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! 向量索引配置类型。

use std::collections::HashMap;

use crate::rabitq::{Metric, RotatorType};
use rootcause::{bail, report};

type Result<T> = std::result::Result<T, rootcause::Report>;

/// 向量索引配置。
///
/// 示例解析格式（key = "vector_index_columns"，value 为逗号分隔）：
/// ```text
/// emb:768:256:7:L2:FhtKac, emb2:512:128:7:IP:Matrix
/// ```
/// 字段顺序：`col:dim:nlist:total_bits:metric:rotator_type`
/// - `metric`: `L2`（默认，欧氏距离）或 `IP`（内积）
/// - `rotator_type`: `FhtKac`（默认，FHT 快速旋转）或 `Matrix`（Gram-Schmidt 正交矩阵）
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

impl VectorIndexConfig {
    /// 从 options map 中解析多个向量列配置。
    ///
    /// 输入格式（key = `"vector_index_columns"`）：
    /// ```text
    /// col:dim:nlist:total_bits:metric:rotator_type:seed:faster
    /// ```
    /// 只有 `col` 和 `dim` 是必填项。
    ///
    /// 多个列配置用逗号分隔。
    pub fn parse_multiple(options: &HashMap<String, String>) -> Result<Vec<Self>> {
        let columns_str = match options.get("vector_index_columns") {
            Some(s) => s.clone(),
            None => return Ok(Vec::new()),
        };

        if columns_str.trim().is_empty() {
            return Ok(Vec::new());
        }

        let mut configs = Vec::new();
        for part in columns_str.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let fields: Vec<&str> = part.split(':').map(|s| s.trim()).collect();
            if fields.len() < 2 {
                bail!(
                    "invalid vector_index_columns entry '{}': expected at least col:dim",
                    part
                );
            }

            let column_name = fields[0].to_string();
            let dim: usize = fields[1].parse().map_err(|_| {
                report!("invalid dim '{}' in entry '{}'", fields[1], part)
            })?;
            let nlist: usize = fields
                .get(2)
                .map(|s| s.parse::<usize>())
                .unwrap_or(Ok(256))
                .map_err(|_| report!("invalid nlist in entry '{}'", part))?;
            let total_bits: usize = fields
                .get(3)
                .map(|s| s.parse::<usize>())
                .unwrap_or(Ok(7))
                .map_err(|_| report!("invalid total_bits in entry '{}'", part))?;
            let metric = match fields.get(4).copied().unwrap_or("L2") {
                "L2" | "l2" => Metric::L2,
                "IP" | "ip" | "InnerProduct" | "innerproduct" => Metric::InnerProduct,
                other => bail!("unknown metric '{}' in entry '{}'", other, part),
            };
            let rotator_type = match fields.get(5).copied().unwrap_or("FhtKac") {
                "FhtKac" | "fhtkac" | "FHT" | "fht" => RotatorType::FhtKacRotator,
                "Matrix" | "matrix" => RotatorType::MatrixRotator,
                other => bail!("unknown rotator_type '{}' in entry '{}'", other, part),
            };
            let seed: u64 = fields
                .get(6)
                .map(|s| s.parse::<u64>())
                .unwrap_or(Ok(42))
                .map_err(|_| report!("invalid seed in entry '{}'", part))?;
            let use_faster_config: bool = fields
                .get(7)
                .map(|s| s.parse::<bool>())
                .unwrap_or(Ok(true))
                .map_err(|_| report!("invalid faster_config in entry '{}'", part))?;

            configs.push(Self {
                column_name,
                dim,
                nlist,
                total_bits,
                metric,
                rotator_type,
                seed,
                use_faster_config,
            });
        }

        Ok(configs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_parse_single_minimal() {
        let mut opts = HashMap::new();
        opts.insert("vector_index_columns".to_string(), "emb:768".to_string());
        let configs = VectorIndexConfig::parse_multiple(&opts).unwrap();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].column_name, "emb");
        assert_eq!(configs[0].dim, 768);
        assert_eq!(configs[0].nlist, 256);
        assert_eq!(configs[0].total_bits, 7);
        assert!(matches!(configs[0].metric, Metric::L2));
    }

    #[test]
    fn test_parse_multiple_full() {
        let mut opts = HashMap::new();
        opts.insert(
            "vector_index_columns".to_string(),
            "emb:768:512:7:L2:FhtKac:42:true, emb2:512:128:8:IP:Matrix:123:false"
                .to_string(),
        );
        let configs = VectorIndexConfig::parse_multiple(&opts).unwrap();
        assert_eq!(configs.len(), 2);

        assert_eq!(configs[0].column_name, "emb");
        assert_eq!(configs[0].dim, 768);
        assert_eq!(configs[0].nlist, 512);
        assert!(matches!(configs[0].metric, Metric::L2));
        assert!(matches!(
            configs[0].rotator_type,
            RotatorType::FhtKacRotator
        ));

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
    fn test_parse_empty() {
        let opts = HashMap::new();
        let configs = VectorIndexConfig::parse_multiple(&opts).unwrap();
        assert!(configs.is_empty());
    }

    #[test]
    fn test_parse_invalid_dim() {
        let mut opts = HashMap::new();
        opts.insert("vector_index_columns".to_string(), "emb:abc".to_string());
        let result = VectorIndexConfig::parse_multiple(&opts);
        assert!(result.is_err());
    }
}
