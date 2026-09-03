use jiff::{
    Zoned,
    tz::{self, TimeZone},
};
use tracing_subscriber::fmt::time::FormatTime;

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
