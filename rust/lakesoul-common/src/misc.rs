use std::{
    collections::HashMap,
    fmt::{self, Debug, Display},
    ops::{Deref, DerefMut},
};

use jiff::{
    Zoned,
    tz::{self},
};
use tracing_subscriber::fmt::time::FormatTime;

pub use jiff::tz::TimeZone;

#[derive(Clone, Default, PartialEq, Eq)]
pub struct SecretMap(HashMap<String, String>);

impl SecretMap {
    const REDACTED: &'static str = "[REDACTED]";

    pub fn new() -> Self {
        Self(HashMap::new())
    }

    fn is_sensitive(key: &str) -> bool {
        matches!(key, "fs.s3a.access.key" | "fs.s3a.secret.key" | "password")
    }

    fn redacted_value<'a>(key: &str, value: &'a str) -> &'a str {
        if Self::is_sensitive(key) {
            Self::REDACTED
        } else {
            value
        }
    }

    pub fn into_inner(self) -> HashMap<String, String> {
        self.0
    }
}

impl Debug for SecretMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();

        for (key, value) in &self.0 {
            map.entry(key, &Self::redacted_value(key, value));
        }

        map.finish()
    }
}

impl Display for SecretMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;

        let mut first = true;

        for (key, value) in &self.0 {
            if !first {
                f.write_str(", ")?;
            }
            first = false;

            write!(f, "{key}={}", Self::redacted_value(key, value))?;
        }

        f.write_str("}")
    }
}

impl Deref for SecretMap {
    type Target = HashMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SecretMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<HashMap<String, String>> for SecretMap {
    fn from(value: HashMap<String, String>) -> Self {
        Self(value)
    }
}

impl From<SecretMap> for HashMap<String, String> {
    fn from(value: SecretMap) -> Self {
        value.0
    }
}

pub struct JiffTime {
    tz: TimeZone,
    fmt: String,
}

impl JiffTime {
    pub fn beijing(fmt: impl Into<String>) -> Self {
        Self {
            tz: TimeZone::fixed(tz::offset(8)), // UTC+8
            fmt: fmt.into(),
        }
    }
    pub fn now(&self) -> Zoned {
        Zoned::now().with_time_zone(self.tz.clone())
    }
    pub fn format(&self) -> String {
        self.now().strftime(&self.fmt).to_string()
    }
}

impl FormatTime for JiffTime {
    fn format_time(
        &self,
        w: &mut tracing_subscriber::fmt::format::Writer<'_>,
    ) -> std::fmt::Result {
        write!(w, "{}", self.format())
    }
}
