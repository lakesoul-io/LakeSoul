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
        // 1.Normalize input: convert to lowercase and trim.
        let input = s.trim().to_lowercase();

        // 2. Parse the input using a regex that matches digits with optional unit suffix.
        // ^(\d+)         -> Matches leading digits
        // \s* -> Allows optional spaces between digits and unit (e.g., "2 GB")
        // ([kmgt]b?|b)?  -> Matches units: k, kb, m, mb, g, gb, t, tb, or just b
        // $              -> Matches the end of the string
        static RE: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^(\d+)\s*([kmgt]b?|b)?$").unwrap());

        let caps = RE.captures(&input).ok_or_else(|| {
            format!(
                "Invalid memory format: '{}'. Expected something like: '2gb', '512MB', or '1024'",
                s
            )
        })?;

        // 3. parsing numbers
        let value = caps[1]
            .parse::<usize>()
            .map_err(|_| format!("数值超限或非法: '{}'", &caps[1]))?;

        // 4. prcessing units
        let unit = caps.get(2).map(|m| m.as_str()).unwrap_or("b");

        let multiplier: usize = match unit {
            "b" => 1,
            "k" | "kb" => 1024,
            "m" | "mb" => 1024 * 1024,
            "g" | "gb" => 1024 * 1024 * 1024,
            "t" | "tb" => 1024 * 1024 * 1024 * 1024,
            _ => 1, // default is B
        };

        // 5. checking for overflow (ex. 999999TB )
        value
            .checked_mul(multiplier)
            .map(ByteSize)
            .ok_or_else(|| "overflow".to_string())
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
        // case-sensitive
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

        // space test
        assert_eq!(
            "2  GB".parse::<ByteSize>().unwrap().bytes(),
            2 * 1024 * 1024 * 1024
        );
        assert_eq!(byte_size!("2  GB"), 2 * 1024 * 1024 * 1024);

        // pure number
        assert_eq!("100".parse::<ByteSize>().unwrap().bytes(), 100);

        // error input
        assert!("abc".parse::<ByteSize>().is_err());
        assert!("2gx".parse::<ByteSize>().is_err());
    }
}
