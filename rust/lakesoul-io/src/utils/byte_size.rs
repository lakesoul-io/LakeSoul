use regex::Regex;
use std::{str::FromStr, sync::LazyLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(usize);

impl ByteSize {
    pub fn bytes(&self) -> usize {
        self.0
    }
}

impl FromStr for ByteSize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // 1. 处理大小写：统一转为小写并去除两端空格
        let input = s.trim().to_lowercase();

        // ^(\d+)  -> 匹配开头的数字
        // \s* -> 允许数字和单位之间有空格 (例如 "2 GB")
        // ([kmgt]b?|b)? -> 匹配单位：k, kb, m, mb, g, gb, t, tb 或纯 b
        // $       -> 匹配结尾
        static RE: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^(\d+)\s*([kmgt]b?|b)?$").unwrap());

        let caps = RE.captures(&input).ok_or_else(|| {
            format!(
                "无法解析内存格式: '{}'。示例格式: '2gb', '512MB', '1024'",
                s
            )
        })?;

        // 3. 解析数字
        let value = caps[1]
            .parse::<usize>()
            .map_err(|_| format!("数值超限或非法: '{}'", &caps[1]))?;

        // 4. 处理单位（此时 unit 已是小写）
        let unit = caps.get(2).map(|m| m.as_str()).unwrap_or("b");

        let multiplier: usize = match unit {
            "b" => 1,
            "k" | "kb" => 1024,
            "m" | "mb" => 1024 * 1024,
            "g" | "gb" => 1024 * 1024 * 1024,
            "t" | "tb" => 1024 * 1024 * 1024 * 1024,
            _ => 1, // 默认 B
        };

        // 5. 检查计算是否溢出 (防止 999999TB 这种输入)
        value
            .checked_mul(multiplier)
            .map(ByteSize)
            .ok_or_else(|| "内存配置数值过大导致溢出".to_string())
    }
}

#[macro_export]
macro_rules! byte_size {
    ($s:expr) => {
        $s.parse::<$crate::utils::ByteSize>()
            .expect("Failed to parse ByteSize from string")
            .bytes()
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_byte_size_parsing() {
        // 大小写测试
        assert_eq!(
            "2GB".parse::<ByteSize>().unwrap().bytes(),
            2 * 1024 * 1024 * 1024
        );
        assert_eq!(byte_size!("2GB"), 2 * 1024 * 1024 * 1024);

        assert_eq!(
            "512mb".parse::<ByteSize>().unwrap().bytes(),
            512 * 1024 * 1024
        );
        assert_eq!(byte_size!("512mb"), 512 * 1024 * 1024);

        assert_eq!("1024Kb".parse::<ByteSize>().unwrap().bytes(), 1024 * 1024);
        assert_eq!(byte_size!("1024Kb"), 1024 * 1024);

        // 空格测试
        assert_eq!(
            "2  GB".parse::<ByteSize>().unwrap().bytes(),
            2 * 1024 * 1024 * 1024
        );
        assert_eq!(byte_size!("2  GB"), 2 * 1024 * 1024 * 1024);

        // 纯数字测试 (默认为 Bytes)
        assert_eq!("100".parse::<ByteSize>().unwrap().bytes(), 100);

        // 错误输入测试
        assert!("abc".parse::<ByteSize>().is_err());
        assert!("2gx".parse::<ByteSize>().is_err());
    }
}
