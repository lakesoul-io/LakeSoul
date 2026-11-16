//! Sqllogictest parser.

use std::fmt;
use std::iter::Peekable;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use regex::Regex;

use crate::ColumnType;

const RESULTS_DELIMITER: &str = "----";

/// The location in source file.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Location {
    file: Arc<str>,
    line: u32,
    upper: Option<Arc<Location>>,
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.file, self.line)?;
        if let Some(upper) = &self.upper {
            write!(f, "\nat {upper}")?;
        }
        Ok(())
    }
}

impl Location {
    /// File path.
    pub fn file(&self) -> &str {
        &self.file
    }

    /// Line number.
    pub fn line(&self) -> u32 {
        self.line
    }

    fn new(file: impl Into<Arc<str>>, line: u32) -> Self {
        Self {
            file: file.into(),
            line,
            upper: None,
        }
    }

    /// Returns the location of next line.
    #[must_use]
    fn next_line(mut self) -> Self {
        self.line += 1;
        self
    }

    /// Returns the location of next level file.
    fn include(&self, file: &str) -> Self {
        Self {
            file: file.into(),
            line: 0,
            upper: Some(Arc::new(self.clone())),
        }
    }
}

/// Configuration for retry behavior
#[derive(Debug, Clone, PartialEq)]
pub struct RetryConfig {
    /// Number of retry attempts
    pub attempts: usize,
    /// Duration to wait between retries
    pub backoff: Duration,
}

/// Expectation for a statement.
#[derive(Debug, Clone, PartialEq)]
pub enum StatementExpect {
    /// Statement should succeed.
    Ok,
    /// Statement should succeed and affect the given number of rows.
    Count(u64),
    /// Statement should fail with the given error message.
    Error(ExpectedError),
}

/// Expectation for a query.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryExpect<T: ColumnType> {
    /// Query should succeed and return the given results.
    Results {
        types: Vec<T>,
        sort_mode: Option<SortMode>,
        result_mode: Option<ResultMode>,
        label: Option<String>,
        results: Vec<String>,
    },
    /// Query should fail with the given error message.
    Error(ExpectedError),
}

impl<T: ColumnType> QueryExpect<T> {
    /// Creates a new [`QueryExpect`] with empty results.
    fn empty_results() -> Self {
        Self::Results {
            types: Vec::new(),
            sort_mode: None,
            result_mode: None,
            label: None,
            results: Vec::new(),
        }
    }
}

/// A single directive in a sqllogictest file.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Record<T: ColumnType> {
    /// An include copies all records from another files.
    Include {
        loc: Location,
        /// A glob pattern
        filename: String,
    },
    /// A statement is an SQL command that is to be evaluated but from which we do not expect to
    /// get results (other than success or failure).
    Statement {
        loc: Location,
        conditions: Vec<Condition>,
        connection: Connection,
        /// The SQL command.
        sql: String,
        expected: StatementExpect,
        /// Optional retry configuration
        retry: Option<RetryConfig>,
    },
    /// A query is an SQL command from which we expect to receive results. The result set might be
    /// empty.
    Query {
        loc: Location,
        conditions: Vec<Condition>,
        connection: Connection,
        /// The SQL command.
        sql: String,
        expected: QueryExpect<T>,
        /// Optional retry configuration
        retry: Option<RetryConfig>,
    },
    /// A system command is an external command that is to be executed by the shell. Currently it
    /// must succeed and the output is ignored.
    #[non_exhaustive]
    System {
        loc: Location,
        conditions: Vec<Condition>,
        /// The external command.
        command: String,
        stdout: Option<String>,
        /// Optional retry configuration
        retry: Option<RetryConfig>,
    },
    /// A sleep period.
    Sleep {
        loc: Location,
        duration: Duration,
    },
    /// Subtest.
    Subtest {
        loc: Location,
        name: String,
    },
    /// A halt record merely causes sqllogictest to ignore the rest of the test script.
    /// For debugging use only.
    Halt {
        loc: Location,
    },
    /// Control statements.
    Control(Control),
    /// Set the maximum number of result values that will be accepted
    /// for a query.  If the number of result values exceeds this number,
    /// then an MD5 hash is computed of all values, and the resulting hash
    /// is the only result.
    ///
    /// If the threshold is 0, then hashing is never used.
    HashThreshold {
        loc: Location,
        threshold: u64,
    },
    /// Condition statements, including `onlyif` and `skipif`.
    Condition(Condition),
    /// Connection statements to specify the connection to use for the following statement.
    Connection(Connection),
    Comment(Vec<String>),
    Newline,
    /// Internally injected record which should not occur in the test file.
    Injected(Injected),
    /// A let record binds SQL query results to variables.
    /// The query must return exactly 1 row with N columns matching the N variable names.
    Let {
        loc: Location,
        conditions: Vec<Condition>,
        connection: Connection,
        /// Variable names to bind the results to.
        variables: Vec<String>,
        /// The SQL query to execute.
        sql: String,
    },
}

impl<T: ColumnType> Record<T> {
    /// Unparses the record to its string representation in the test file.
    ///
    /// # Panics
    /// If the record is an internally injected record which should not occur in the test file.
    pub fn unparse(&self, w: &mut impl std::io::Write) -> std::io::Result<()> {
        write!(w, "{self}")
    }
}

/// As is the standard for Display, does not print any trailing
/// newline except for records that always end with a blank line such
/// as Query and Statement.
impl<T: ColumnType> std::fmt::Display for Record<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Record::Include { loc: _, filename } => {
                write!(f, "include {filename}")
            }
            Record::Statement {
                loc: _,
                conditions: _,
                connection: _,
                sql,
                expected,
                retry,
            } => {
                write!(f, "statement ")?;
                match expected {
                    StatementExpect::Ok => write!(f, "ok")?,
                    StatementExpect::Count(cnt) => write!(f, "count {cnt}")?,
                    StatementExpect::Error(err) => err.fmt_inline(f)?,
                }
                if let Some(retry) = retry {
                    write!(
                        f,
                        " retry {} backoff {}",
                        retry.attempts,
                        humantime::format_duration(retry.backoff)
                    )?;
                }
                writeln!(f)?;
                // statement always end with a blank line
                writeln!(f, "{sql}")?;

                if let StatementExpect::Error(err) = expected {
                    err.fmt_multiline(f)?;
                }
                Ok(())
            }
            Record::Query {
                loc: _,
                conditions: _,
                connection: _,
                sql,
                expected,
                retry,
            } => {
                write!(f, "query ")?;
                match expected {
                    QueryExpect::Results {
                        types,
                        sort_mode,
                        label,
                        ..
                    } => {
                        write!(f, "{}", types.iter().map(|c| c.to_char()).join(""))?;
                        if let Some(sort_mode) = sort_mode {
                            write!(f, " {}", sort_mode.as_str())?;
                        }
                        if let Some(label) = label {
                            write!(f, " {label}")?;
                        }
                    }
                    QueryExpect::Error(err) => err.fmt_inline(f)?,
                }
                if let Some(retry) = retry {
                    write!(
                        f,
                        " retry {} backoff {}",
                        retry.attempts,
                        humantime::format_duration(retry.backoff)
                    )?;
                }
                writeln!(f)?;
                writeln!(f, "{sql}")?;

                match expected {
                    QueryExpect::Results { results, .. } => {
                        write!(f, "{}", RESULTS_DELIMITER)?;
                        for result in results {
                            write!(f, "\n{result}")?;
                        }

                        // query always ends with a blank line
                        writeln!(f)?
                    }
                    QueryExpect::Error(err) => err.fmt_multiline(f)?,
                }
                Ok(())
            }
            Record::System {
                loc: _,
                conditions: _,
                command,
                stdout,
                retry,
            } => {
                writeln!(f, "system ok\n{command}")?;
                if let Some(retry) = retry {
                    write!(
                        f,
                        " retry {} backoff {}",
                        retry.attempts,
                        humantime::format_duration(retry.backoff)
                    )?;
                }
                if let Some(stdout) = stdout {
                    writeln!(f, "----\n{}\n", stdout.trim())?;
                }
                Ok(())
            }
            Record::Sleep { loc: _, duration } => {
                write!(f, "sleep {}", humantime::format_duration(*duration))
            }
            Record::Subtest { loc: _, name } => {
                write!(f, "subtest {name}")
            }
            Record::Halt { loc: _ } => {
                write!(f, "halt")
            }
            Record::Control(c) => match c {
                Control::SortMode(m) => write!(f, "control sortmode {}", m.as_str()),
                Control::ResultMode(m) => write!(f, "control resultmode {}", m.as_str()),
                Control::Substitution(s) => write!(f, "control substitution {}", s.as_str()),
            },
            Record::Condition(cond) => match cond {
                Condition::OnlyIf { label } => write!(f, "onlyif {label}"),
                Condition::SkipIf { label } => write!(f, "skipif {label}"),
            },
            Record::Connection(conn) => {
                if let Connection::Named(conn) = conn {
                    write!(f, "connection {}", conn)?;
                }
                Ok(())
            }
            Record::HashThreshold { loc: _, threshold } => {
                write!(f, "hash-threshold {threshold}")
            }
            Record::Comment(comment) => {
                let mut iter = comment.iter();
                write!(f, "#{}", iter.next().unwrap().trim_end())?;
                for line in iter {
                    write!(f, "\n#{}", line.trim_end())?;
                }
                Ok(())
            }
            Record::Newline => Ok(()), // Display doesn't end with newline
            Record::Injected(p) => panic!("unexpected injected record: {p:?}"),
            Record::Let {
                loc: _,
                conditions: _,
                connection: _,
                variables,
                sql,
            } => {
                write!(f, "let {}\n{sql}\n", variables.join(", "))
            }
        }
    }
}

/// Expected error message after `error` or under `----`.
#[derive(Debug, Clone)]
pub enum ExpectedError {
    /// No expected error message.
    ///
    /// Any error message is considered as a match.
    Empty,
    /// An inline regular expression after `error`.
    ///
    /// The actual error message that matches the regex is considered as a match.
    Inline(Regex),
    /// A multiline error message under `----`, ends with 2 consecutive empty lines.
    ///
    /// The actual error message that's exactly the same as the expected one is considered as a
    /// match.
    Multiline(String),
    /// An expected SQL state code.
    ///
    /// The actual SQL state that matches the expected one is considered as a match.
    SqlState(String),
}

impl ExpectedError {
    /// Parses an inline regex variant from tokens.
    fn parse_inline_tokens(tokens: &[&str]) -> Result<Self, ParseErrorKind> {
        let joined = tokens.join(" ");

        // Check if this is a SQLSTATE error pattern: error(<SQLSTATE>).
        // SQLSTATE is exactly 5 characters: digits and uppercase letters (SQL standard).
        if let Some(captures) = regex::Regex::new(r"^\(([0-9A-Z]{5})\)$")
            .unwrap()
            .captures(&joined)
        {
            if let Some(sqlstate) = captures.get(1) {
                return Ok(Self::SqlState(sqlstate.as_str().to_string()));
            }
        }

        Self::new_inline(joined)
    }

    /// Creates an inline expected error message from a regex string.
    ///
    /// If the regex is empty, it's considered as [`ExpectedError::Empty`].
    fn new_inline(regex: String) -> Result<Self, ParseErrorKind> {
        if regex.is_empty() {
            Ok(Self::Empty)
        } else {
            let regex =
                Regex::new(&regex).map_err(|_| ParseErrorKind::InvalidErrorMessage(regex))?;
            Ok(Self::Inline(regex))
        }
    }

    /// Returns whether it's an empty match.
    fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    /// Unparses the expected message after `statement`.
    fn fmt_inline(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "error")?;
        match self {
            Self::Inline(regex) => write!(f, " {regex}")?,
            Self::SqlState(sqlstate) => write!(f, " ({sqlstate})")?,
            Self::Empty | Self::Multiline(_) => {}
        }
        Ok(())
    }

    /// Unparses the expected message with `----`, if it's multiline.
    fn fmt_multiline(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Self::Multiline(results) = self {
            writeln!(f, "{}", RESULTS_DELIMITER)?;
            writeln!(f, "{}", results.trim())?;
            writeln!(f)?; // another empty line to indicate the end of multiline message
        }
        Ok(())
    }

    /// Returns whether the given error message matches the expected one.
    pub fn is_match(&self, err: &str, sqlstate: Option<&str>) -> bool {
        match self {
            Self::Empty => true,
            Self::Inline(regex) => regex.is_match(err),
            Self::Multiline(results) => results.trim() == err.trim(),
            Self::SqlState(expected_state) => sqlstate.is_some_and(|state| state == expected_state),
        }
    }

    /// Creates an expected error message from the actual error message. Used by the runner
    /// to update the test cases with `--override`.
    ///
    /// A reference might be provided to help decide whether to use inline or multiline.
    pub fn from_actual_error(reference: Option<&Self>, actual_err: &str) -> Self {
        let trimmed_err = actual_err.trim();
        let err_is_multiline = trimmed_err.lines().next_tuple::<(_, _)>().is_some();

        let multiline = match reference {
            Some(Self::Multiline(_)) => true, // always multiline if the ref is multiline
            _ => err_is_multiline,            // prefer inline as long as it fits
        };

        if multiline {
            // Even if the actual error is empty, we still use `Multiline` to indicate that
            // an exact empty error is expected, instead of any error by `Empty`.
            Self::Multiline(trimmed_err.to_string())
        } else {
            Self::new_inline(regex::escape(actual_err)).expect("escaped regex should be valid")
        }
    }
}

impl std::fmt::Display for ExpectedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExpectedError::Empty => write!(f, "(any)"),
            ExpectedError::Inline(regex) => write!(f, "(regex) {}", regex),
            ExpectedError::Multiline(results) => write!(f, "(multiline) {}", results.trim()),
            ExpectedError::SqlState(sqlstate) => write!(f, "(sqlstate) {}", sqlstate),
        }
    }
}

impl PartialEq for ExpectedError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Empty, Self::Empty) => true,
            (Self::Inline(l0), Self::Inline(r0)) => l0.as_str() == r0.as_str(),
            (Self::Multiline(l0), Self::Multiline(r0)) => l0 == r0,
            (Self::SqlState(l0), Self::SqlState(r0)) => l0 == r0,
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[non_exhaustive]
pub enum Control {
    /// Control sort mode.
    SortMode(SortMode),
    /// control result mode.
    ResultMode(ResultMode),
    /// Control whether or not to substitute variables in the SQL.
    Substitution(bool),
}

trait ControlItem: Sized {
    /// Try to parse from string.
    fn try_from_str(s: &str) -> Result<Self, ParseErrorKind>;

    /// Convert to string.
    fn as_str(&self) -> &'static str;
}

impl ControlItem for bool {
    fn try_from_str(s: &str) -> Result<Self, ParseErrorKind> {
        match s {
            "on" => Ok(true),
            "off" => Ok(false),
            _ => Err(ParseErrorKind::InvalidControl(s.to_string())),
        }
    }

    fn as_str(&self) -> &'static str {
        if *self {
            "on"
        } else {
            "off"
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Injected {
    /// Pseudo control command to indicate the begin of an include statement. Automatically
    /// injected by sqllogictest parser.
    BeginInclude(String),
    /// Pseudo control command to indicate the end of an include statement. Automatically injected
    /// by sqllogictest parser.
    EndInclude(String),
}

/// The condition to run a query.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Condition {
    /// The statement or query is evaluated only if the label is seen.
    OnlyIf { label: String },
    /// The statement or query is not evaluated if the label is seen.
    SkipIf { label: String },
}

impl Condition {
    /// Evaluate condition on given `label`, returns whether to skip this record.
    pub(crate) fn should_skip<'a>(&'a self, labels: impl IntoIterator<Item = &'a str>) -> bool {
        match self {
            Condition::OnlyIf { label } => !labels.into_iter().contains(&label.as_str()),
            Condition::SkipIf { label } => labels.into_iter().contains(&label.as_str()),
        }
    }
}

/// The connection to use for the following statement.
#[derive(Default, Debug, PartialEq, Eq, Hash, Clone)]
pub enum Connection {
    /// The default connection if not specified or if the name is "default".
    #[default]
    Default,
    /// A named connection.
    Named(String),
}

impl Connection {
    fn new(name: impl AsRef<str>) -> Self {
        match name.as_ref() {
            "default" => Self::Default,
            name => Self::Named(name.to_owned()),
        }
    }
}

/// Whether to apply sorting before checking the results of a query.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SortMode {
    /// The default option. The results appear in exactly the order in which they were received
    /// from the database engine.
    NoSort,
    /// Gathers all output from the database engine then sorts it by rows.
    RowSort,
    /// It works like rowsort except that it does not honor row groupings. Each individual result
    /// value is sorted on its own.
    ValueSort,
}

impl ControlItem for SortMode {
    fn try_from_str(s: &str) -> Result<Self, ParseErrorKind> {
        match s {
            "nosort" => Ok(Self::NoSort),
            "rowsort" => Ok(Self::RowSort),
            "valuesort" => Ok(Self::ValueSort),
            _ => Err(ParseErrorKind::InvalidSortMode(s.to_string())),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::NoSort => "nosort",
            Self::RowSort => "rowsort",
            Self::ValueSort => "valuesort",
        }
    }
}

/// Whether the results should be parsed as value-wise or row-wise
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ResultMode {
    /// Results are in a single column
    ValueWise,
    /// The default option where results are in columns separated by spaces
    RowWise,
}

impl ControlItem for ResultMode {
    fn try_from_str(s: &str) -> Result<Self, ParseErrorKind> {
        match s {
            "rowwise" => Ok(Self::RowWise),
            "valuewise" => Ok(Self::ValueWise),
            _ => Err(ParseErrorKind::InvalidSortMode(s.to_string())),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::RowWise => "rowwise",
            Self::ValueWise => "valuewise",
        }
    }
}

impl fmt::Display for ResultMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// The error type for parsing sqllogictest.
#[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
#[error("parse error at {loc}: {kind}")]
pub struct ParseError {
    kind: ParseErrorKind,
    loc: Location,
}

impl ParseError {
    /// Returns the corresponding [`ParseErrorKind`] for this error.
    pub fn kind(&self) -> ParseErrorKind {
        self.kind.clone()
    }

    /// Returns the location from which the error originated.
    pub fn location(&self) -> Location {
        self.loc.clone()
    }
}

/// The error kind for parsing sqllogictest.
#[derive(thiserror::Error, Debug, Eq, PartialEq, Clone)]
#[non_exhaustive]
pub enum ParseErrorKind {
    #[error("unexpected token: {0:?}")]
    UnexpectedToken(String),
    #[error("unexpected EOF")]
    UnexpectedEOF,
    #[error("invalid sort mode: {0:?}")]
    InvalidSortMode(String),
    #[error("invalid line: {0:?}")]
    InvalidLine(String),
    #[error("invalid type character: {0:?} in type string")]
    InvalidType(char),
    #[error("invalid number: {0:?}")]
    InvalidNumber(String),
    #[error("invalid error message: {0:?}")]
    InvalidErrorMessage(String),
    #[error("duplicated error messages after error` and under `----`")]
    DuplicatedErrorMessage,
    #[error("invalid retry config: {0:?}")]
    InvalidRetryConfig(String),
    #[error("statement should have no result, use `query` instead")]
    StatementHasResults,
    #[error("invalid duration: {0:?}")]
    InvalidDuration(String),
    #[error("invalid control: {0:?}")]
    InvalidControl(String),
    #[error("invalid include file pattern: {0}")]
    InvalidIncludeFile(String),
    #[error("no files found for include file pattern: {0:?}")]
    EmptyIncludeFile(String),
    #[error("no such file")]
    FileNotFound,
    #[error("invalid variable name: {0:?}")]
    InvalidVariableName(String),
}

impl ParseErrorKind {
    fn at(self, loc: Location) -> ParseError {
        ParseError { kind: self, loc }
    }
}

/// Parse a sqllogictest script into a list of records.
pub fn parse<T: ColumnType>(script: &str) -> Result<Vec<Record<T>>, ParseError> {
    parse_inner(&Location::new("<unknown>", 0), script)
}

/// Parse a sqllogictest script into a list of records with a given script name.
pub fn parse_with_name<T: ColumnType>(
    script: &str,
    name: impl Into<Arc<str>>,
) -> Result<Vec<Record<T>>, ParseError> {
    parse_inner(&Location::new(name, 0), script)
}

#[allow(clippy::collapsible_match)]
fn parse_inner<T: ColumnType>(loc: &Location, script: &str) -> Result<Vec<Record<T>>, ParseError> {
    let mut lines = script.lines().enumerate().peekable();
    let mut records = vec![];
    let mut conditions = vec![];
    let mut connection = Connection::Default;
    let mut comments = vec![];

    while let Some((num, line)) = lines.next() {
        if let Some(text) = line.strip_prefix('#') {
            comments.push(text.to_string());
            if lines.peek().is_none() {
                // Special handling for the case where the last line is a comment.
                records.push(Record::Comment(comments));
                break;
            }
            continue;
        }
        if !comments.is_empty() {
            records.push(Record::Comment(comments));
            comments = vec![];
        }

        if line.is_empty() {
            records.push(Record::Newline);
            continue;
        }

        let mut loc = loc.clone();
        loc.line = num as u32 + 1;

        let tokens: Vec<&str> = line.split_whitespace().collect();
        match tokens.as_slice() {
            [] => continue,
            ["include", included] => records.push(Record::Include {
                loc,
                filename: included.to_string(),
            }),
            ["halt"] => {
                records.push(Record::Halt { loc });
            }
            ["subtest", name] => {
                records.push(Record::Subtest {
                    loc,
                    name: name.to_string(),
                });
            }
            ["sleep", dur] => {
                records.push(Record::Sleep {
                    duration: humantime::parse_duration(dur).map_err(|_| {
                        ParseErrorKind::InvalidDuration(dur.to_string()).at(loc.clone())
                    })?,
                    loc,
                });
            }
            ["skipif", label] => {
                let cond = Condition::SkipIf {
                    label: label.to_string(),
                };
                conditions.push(cond.clone());
                records.push(Record::Condition(cond));
            }
            ["onlyif", label] => {
                let cond = Condition::OnlyIf {
                    label: label.to_string(),
                };
                conditions.push(cond.clone());
                records.push(Record::Condition(cond));
            }
            ["connection", name] => {
                let conn = Connection::new(name);
                connection = conn.clone();
                records.push(Record::Connection(conn));
            }
            ["statement", res @ ..] => {
                let (mut expected, res) = match res {
                    ["ok", retry @ ..] => (StatementExpect::Ok, retry),
                    ["error", res @ ..] => {
                        if res.len() == 4 && res[0] == "retry" && res[2] == "backoff" {
                            // `statement error retry <num> backoff <duration>`
                            // To keep syntax simple, let's assume the error message must be multiline.
                            (StatementExpect::Error(ExpectedError::Empty), res)
                        } else {
                            let error = ExpectedError::parse_inline_tokens(res)
                                .map_err(|e| e.at(loc.clone()))?;
                            (StatementExpect::Error(error), &[][..])
                        }
                    }
                    ["count", count_str, retry @ ..] => {
                        let count = count_str.parse::<u64>().map_err(|_| {
                            ParseErrorKind::InvalidNumber((*count_str).into()).at(loc.clone())
                        })?;
                        (StatementExpect::Count(count), retry)
                    }
                    _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
                };

                let retry = parse_retry_config(res).map_err(|e| e.at(loc.clone()))?;

                let (sql, has_results) = parse_lines(&mut lines, &loc, Some(RESULTS_DELIMITER))?;

                if has_results {
                    if let StatementExpect::Error(e) = &mut expected {
                        // If no inline error message is specified, it might be a multiline error.
                        if e.is_empty() {
                            *e = parse_multiline_error(&mut lines);
                        } else {
                            return Err(ParseErrorKind::DuplicatedErrorMessage.at(loc.clone()));
                        }
                    } else {
                        return Err(ParseErrorKind::StatementHasResults.at(loc.clone()));
                    }
                }

                records.push(Record::Statement {
                    loc,
                    conditions: std::mem::take(&mut conditions),
                    connection: std::mem::take(&mut connection),
                    sql,
                    expected,
                    retry,
                });
            }
            ["query", res @ ..] => {
                let (mut expected, res) = match res {
                    ["error", res @ ..] => {
                        if res.len() == 4 && res[0] == "retry" && res[2] == "backoff" {
                            // `query error retry <num> backoff <duration>`
                            // To keep syntax simple, let's assume the error message must be multiline.
                            (QueryExpect::Error(ExpectedError::Empty), res)
                        } else {
                            let error = ExpectedError::parse_inline_tokens(res)
                                .map_err(|e| e.at(loc.clone()))?;
                            (QueryExpect::Error(error), &[][..])
                        }
                    }
                    [type_str, res @ ..] => {
                        // query <type-string> [<sort-mode>] [<label>] [retry <attempts> backoff <backoff>]
                        let types = type_str
                            .chars()
                            .map(|ch| {
                                T::from_char(ch)
                                    .ok_or_else(|| ParseErrorKind::InvalidType(ch).at(loc.clone()))
                            })
                            .try_collect()?;
                        let sort_mode = res.first().and_then(|&s| SortMode::try_from_str(s).ok()); // Could be `retry` or label

                        // To support `retry`, we assume the label must *not* be "retry"
                        let label_start = if sort_mode.is_some() { 1 } else { 0 };
                        let res = &res[label_start..];
                        let label = res.first().and_then(|&s| {
                            if s != "retry" {
                                Some(s.to_owned())
                            } else {
                                None // `retry` is not a valid label
                            }
                        });

                        let retry_start = if label.is_some() { 1 } else { 0 };
                        let res = &res[retry_start..];
                        (
                            QueryExpect::Results {
                                types,
                                sort_mode,
                                result_mode: None,
                                label,
                                results: Vec::new(),
                            },
                            res,
                        )
                    }
                    [] => (QueryExpect::empty_results(), &[][..]),
                };

                let retry = parse_retry_config(res).map_err(|e| e.at(loc.clone()))?;

                // The SQL for the query is found on second and subsequent lines of the record
                // up to first line of the form "----" or until the end of the record.
                let (sql, has_result) = parse_lines(&mut lines, &loc, Some(RESULTS_DELIMITER))?;
                if has_result {
                    match &mut expected {
                        // Lines following the "----" are expected results of the query, one value
                        // per line.
                        QueryExpect::Results { results, .. } => {
                            for (_, line) in &mut lines {
                                if line.is_empty() {
                                    break;
                                }
                                results.push(line.to_string());
                            }
                        }
                        // If no inline error message is specified, it might be a multiline error.
                        QueryExpect::Error(e) => {
                            if e.is_empty() {
                                *e = parse_multiline_error(&mut lines);
                            } else {
                                return Err(ParseErrorKind::DuplicatedErrorMessage.at(loc.clone()));
                            }
                        }
                    }
                }
                records.push(Record::Query {
                    loc,
                    conditions: std::mem::take(&mut conditions),
                    connection: std::mem::take(&mut connection),
                    sql,
                    expected,
                    retry,
                });
            }
            ["system", "ok", res @ ..] => {
                let retry = parse_retry_config(res).map_err(|e| e.at(loc.clone()))?;

                // TODO: we don't support asserting error message for system command
                // The command is found on second and subsequent lines of the record
                // up to first line of the form "----" or until the end of the record.
                let (command, has_result) = parse_lines(&mut lines, &loc, Some(RESULTS_DELIMITER))?;
                let stdout = if has_result {
                    Some(parse_multiple_result(&mut lines))
                } else {
                    None
                };
                records.push(Record::System {
                    loc,
                    conditions: std::mem::take(&mut conditions),
                    command,
                    stdout,
                    retry,
                });
            }
            ["control", res @ ..] => match res {
                ["resultmode", result_mode] => match ResultMode::try_from_str(result_mode) {
                    Ok(result_mode) => {
                        records.push(Record::Control(Control::ResultMode(result_mode)))
                    }
                    Err(k) => return Err(k.at(loc)),
                },
                ["sortmode", sort_mode] => match SortMode::try_from_str(sort_mode) {
                    Ok(sort_mode) => records.push(Record::Control(Control::SortMode(sort_mode))),
                    Err(k) => return Err(k.at(loc)),
                },
                ["substitution", on_off] => match bool::try_from_str(on_off) {
                    Ok(on_off) => records.push(Record::Control(Control::Substitution(on_off))),
                    Err(k) => return Err(k.at(loc)),
                },
                _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
            },
            ["hash-threshold", threshold] => {
                records.push(Record::HashThreshold {
                    loc: loc.clone(),
                    threshold: threshold.parse::<u64>().map_err(|_| {
                        ParseErrorKind::InvalidNumber((*threshold).into()).at(loc.clone())
                    })?,
                });
            }
            ["let", rest @ ..] => {
                // Parse: let var1, var2, ... followed by SQL
                // Join the rest of the tokens and parse the variable list
                let rest_str = rest.join(" ");
                let rest_str = rest_str.trim();

                if rest_str.is_empty() {
                    return Err(ParseErrorKind::InvalidLine(line.into()).at(loc));
                }

                // Extract variable names separated by commas
                let variables: Vec<String> = rest_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();

                if variables.is_empty() {
                    return Err(ParseErrorKind::InvalidLine(line.into()).at(loc));
                }

                // Validate variable names (must be valid identifiers)
                for var in &variables {
                    if !is_valid_variable_name(var) {
                        return Err(ParseErrorKind::InvalidVariableName(var.clone()).at(loc));
                    }
                }

                // Parse the SQL body (following lines until empty line)
                let (sql, _has_results) = parse_lines(&mut lines, &loc, None)?;

                records.push(Record::Let {
                    loc,
                    conditions: std::mem::take(&mut conditions),
                    connection: std::mem::take(&mut connection),
                    variables,
                    sql,
                });
            }
            _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
        }
    }
    Ok(records)
}

/// Parse a sqllogictest file. The included scripts are inserted after the `include` record.
pub fn parse_file<T: ColumnType>(filename: impl AsRef<Path>) -> Result<Vec<Record<T>>, ParseError> {
    let filename = filename.as_ref().to_str().unwrap();
    parse_file_inner(Location::new(filename, 0))
}

fn parse_file_inner<T: ColumnType>(loc: Location) -> Result<Vec<Record<T>>, ParseError> {
    let path = Path::new(loc.file());
    if !path.exists() {
        return Err(ParseErrorKind::FileNotFound.at(loc.clone()));
    }
    let script = std::fs::read_to_string(path).unwrap();
    let mut records = vec![];
    for rec in parse_inner(&loc, &script)? {
        records.push(rec.clone());

        if let Record::Include { filename, loc } = rec {
            let complete_filename = {
                let mut path_buf = path.to_path_buf();
                path_buf.pop();
                path_buf.push(filename.clone());
                path_buf.as_os_str().to_string_lossy().to_string()
            };

            let mut iter = glob::glob(&complete_filename)
                .map_err(|e| ParseErrorKind::InvalidIncludeFile(e.to_string()).at(loc.clone()))?
                .peekable();
            if iter.peek().is_none() {
                return Err(ParseErrorKind::EmptyIncludeFile(filename).at(loc.clone()));
            }
            for included_file in iter {
                let included_file = included_file.map_err(|e| {
                    ParseErrorKind::InvalidIncludeFile(e.to_string()).at(loc.clone())
                })?;
                let included_file = included_file.as_os_str().to_string_lossy().to_string();

                records.push(Record::Injected(Injected::BeginInclude(
                    included_file.clone(),
                )));
                records.extend(parse_file_inner(loc.include(&included_file))?);
                records.push(Record::Injected(Injected::EndInclude(included_file)));
            }
        }
    }
    Ok(records)
}

/// Check if a variable name is valid.
/// A valid variable name starts with a letter or underscore, and contains only alphanumeric
/// characters and underscores.
fn is_valid_variable_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Parse one or more lines until empty line or a delimiter.
fn parse_lines<'a>(
    lines: &mut impl Iterator<Item = (usize, &'a str)>,
    loc: &Location,
    delimiter: Option<&str>,
) -> Result<(String, bool), ParseError> {
    let mut found_delimiter = false;
    let mut out = match lines.next() {
        Some((_, line)) => Ok(line.into()),
        None => Err(ParseErrorKind::UnexpectedEOF.at(loc.clone().next_line())),
    }?;

    for (_, line) in lines {
        if line.is_empty() {
            break;
        }
        if let Some(delimiter) = delimiter {
            if line == delimiter {
                found_delimiter = true;
                break;
            }
        }
        out += "\n";
        out += line;
    }

    Ok((out, found_delimiter))
}

/// Parse multiline output under `----`.
fn parse_multiple_result<'a>(
    lines: &mut Peekable<impl Iterator<Item = (usize, &'a str)>>,
) -> String {
    let mut results = String::new();

    while let Some((_, line)) = lines.next() {
        // 2 consecutive empty lines
        if line.is_empty() && lines.peek().map(|(_, l)| l.is_empty()).unwrap_or(true) {
            lines.next();
            break;
        }
        results += line;
        results.push('\n');
    }

    results.trim().to_string()
}

/// Parse multiline error message under `----`.
fn parse_multiline_error<'a>(
    lines: &mut Peekable<impl Iterator<Item = (usize, &'a str)>>,
) -> ExpectedError {
    ExpectedError::Multiline(parse_multiple_result(lines))
}

/// Parse retry configuration from tokens
///
/// The retry configuration is optional and can be specified as:
///
/// ```text
/// ... retry 3 backoff 1s
/// ```
fn parse_retry_config(tokens: &[&str]) -> Result<Option<RetryConfig>, ParseErrorKind> {
    if tokens.is_empty() {
        return Ok(None);
    }

    let mut iter = tokens.iter().peekable();

    // Check if we have retry clause
    match iter.next() {
        Some(&"retry") => {}
        Some(token) => return Err(ParseErrorKind::UnexpectedToken(token.to_string())),
        None => return Ok(None),
    }

    // Parse number of attempts
    let attempts = match iter.next() {
        Some(attempts_str) => attempts_str
            .parse::<usize>()
            .map_err(|_| ParseErrorKind::InvalidNumber(attempts_str.to_string()))?,
        None => {
            return Err(ParseErrorKind::InvalidRetryConfig(
                "expected a positive number of attempts".to_string(),
            ))
        }
    };

    if attempts == 0 {
        return Err(ParseErrorKind::InvalidRetryConfig(
            "attempt must be greater than 0".to_string(),
        ));
    }

    // Expect "backoff" keyword
    match iter.next() {
        Some(&"backoff") => {}
        Some(token) => return Err(ParseErrorKind::UnexpectedToken(token.to_string())),
        None => {
            return Err(ParseErrorKind::InvalidRetryConfig(
                "expected keyword backoff".to_string(),
            ))
        }
    }

    // Parse backoff duration
    let duration_str = match iter.next() {
        Some(s) => s,
        None => {
            return Err(ParseErrorKind::InvalidRetryConfig(
                "expected backoff duration".to_string(),
            ))
        }
    };

    let backoff = humantime::parse_duration(duration_str)
        .map_err(|_| ParseErrorKind::InvalidDuration(duration_str.to_string()))?;

    // No more tokens should be present
    if iter.next().is_some() {
        return Err(ParseErrorKind::UnexpectedToken("extra tokens".to_string()));
    }

    Ok(Some(RetryConfig { attempts, backoff }))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::DefaultColumnType;

    #[test]
    fn test_trailing_comment() {
        let script = "\
# comment 1
#  comment 2
";
        let records = parse::<DefaultColumnType>(script).unwrap();
        assert_eq!(
            records,
            vec![Record::Comment(vec![
                " comment 1".to_string(),
                "  comment 2".to_string(),
            ]),]
        );
    }

    #[test]
    fn test_include_glob() {
        let records =
            parse_file::<DefaultColumnType>("../tests/slt/include/include_1.slt").unwrap();
        assert_eq!(15, records.len());
    }

    #[test]
    fn test_basic() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/basic.slt")
    }

    #[test]
    fn test_condition() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/condition.slt")
    }

    #[test]
    fn test_file_level_sort_mode() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/file_level_sort_mode.slt")
    }

    #[test]
    fn test_rowsort() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/rowsort.slt")
    }

    #[test]
    fn test_valuesort() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/valuesort.slt")
    }

    #[test]
    fn test_substitution() {
        parse_roundtrip::<DefaultColumnType>("../tests/substitution/basic.slt")
    }

    #[test]
    fn test_test_dir_escape() {
        parse_roundtrip::<DefaultColumnType>("../tests/test_dir_escape/test_dir_escape.slt")
    }

    #[test]
    fn test_validator() {
        parse_roundtrip::<DefaultColumnType>("../tests/validator/validator.slt")
    }

    #[test]
    fn test_custom_type() {
        parse_roundtrip::<CustomColumnType>("../tests/custom_type/custom_type.slt")
    }

    #[test]
    fn test_system_command() {
        parse_roundtrip::<DefaultColumnType>("../tests/system_command/system_command.slt")
    }

    #[test]
    fn test_fail_unknown_type() {
        let script = "\
query IA
select * from unknown_type
----
";

        let error_kind = parse::<CustomColumnType>(script).unwrap_err().kind;

        assert_eq!(error_kind, ParseErrorKind::InvalidType('A'));
    }

    #[test]
    fn test_parse_no_types() {
        let script = "\
query
select * from foo;
----
";
        let records = parse::<DefaultColumnType>(script).unwrap();

        assert_eq!(
            records,
            vec![Record::Query {
                loc: Location::new("<unknown>", 1),
                conditions: vec![],
                connection: Connection::Default,
                sql: "select * from foo;".to_string(),
                expected: QueryExpect::empty_results(),
                retry: None,
            }]
        );
    }

    #[test]
    fn test_expected_error_sqlstate_format() {
        assert!(matches!(
            ExpectedError::parse_inline_tokens(&["(42P01)"]).unwrap(),
            ExpectedError::SqlState(s) if s == "42P01"
        ));
        assert!(matches!(
            ExpectedError::parse_inline_tokens(&["(HY000)"]).unwrap(),
            ExpectedError::SqlState(s) if s == "HY000"
        ));

        for non_sqlstate in ["(42p01)", "(42P0)", "(42P011)", "(12_45)", "(12-45)"] {
            assert!(matches!(
                ExpectedError::parse_inline_tokens(&[non_sqlstate]).unwrap(),
                ExpectedError::Inline(_)
            ));
        }
    }

    /// Verifies Display impl is consistent with parsing by ensuring
    /// roundtrip parse(unparse(parse())) is consistent
    #[track_caller]
    fn parse_roundtrip<T: ColumnType>(filename: impl AsRef<Path>) {
        let filename = filename.as_ref();
        let records = parse_file::<T>(filename).expect("parsing to complete");

        let unparsed = records
            .iter()
            .map(|record| record.to_string())
            .collect::<Vec<_>>();

        let output_contents = unparsed.join("\n");

        // The original and parsed records should be logically equivalent
        let mut output_file = tempfile::NamedTempFile::new().expect("Error creating tempfile");
        output_file
            .write_all(output_contents.as_bytes())
            .expect("Unable to write file");
        output_file.flush().unwrap();

        let output_path = output_file.into_temp_path();
        let reparsed_records =
            parse_file(&output_path).expect("reparsing to complete successfully");

        let records = normalize_filename(records);
        let reparsed_records = normalize_filename(reparsed_records);

        pretty_assertions::assert_eq!(records, reparsed_records, "Mismatch in reparsed records");
    }

    /// Replaces the actual filename in all Records with
    /// "__FILENAME__" so different files with the same contents can
    /// compare equal
    fn normalize_filename<T: ColumnType>(records: Vec<Record<T>>) -> Vec<Record<T>> {
        records
            .into_iter()
            .map(|mut record| {
                match &mut record {
                    Record::Include { loc, .. } => normalize_loc(loc),
                    Record::Statement { loc, .. } => normalize_loc(loc),
                    Record::System { loc, .. } => normalize_loc(loc),
                    Record::Query { loc, .. } => normalize_loc(loc),
                    Record::Sleep { loc, .. } => normalize_loc(loc),
                    Record::Subtest { loc, .. } => normalize_loc(loc),
                    Record::Halt { loc, .. } => normalize_loc(loc),
                    Record::HashThreshold { loc, .. } => normalize_loc(loc),
                    Record::Let { loc, .. } => normalize_loc(loc),
                    // even though these variants don't include a
                    // location include them in this match statement
                    // so if new variants are added, this match
                    // statement must be too.
                    Record::Condition(_)
                    | Record::Connection(_)
                    | Record::Comment(_)
                    | Record::Control(_)
                    | Record::Newline
                    | Record::Injected(_) => {}
                };
                record
            })
            .collect()
    }

    // Normalize a location
    fn normalize_loc(loc: &mut Location) {
        loc.file = Arc::from("__FILENAME__");
    }

    #[derive(Debug, PartialEq, Eq, Clone)]
    pub enum CustomColumnType {
        Integer,
        Boolean,
    }

    impl ColumnType for CustomColumnType {
        fn from_char(value: char) -> Option<Self> {
            match value {
                'I' => Some(Self::Integer),
                'B' => Some(Self::Boolean),
                _ => None,
            }
        }

        fn to_char(&self) -> char {
            match self {
                Self::Integer => 'I',
                Self::Boolean => 'B',
            }
        }
    }

    #[test]
    fn test_statement_retry() {
        parse_roundtrip::<DefaultColumnType>("../tests/no_run/statement_retry.slt")
    }

    #[test]
    fn test_query_retry() {
        parse_roundtrip::<DefaultColumnType>("../tests/no_run/query_retry.slt")
    }

    #[test]
    fn test_let_parsing() {
        let script = "\
let id
SELECT 1
";
        let records = parse::<DefaultColumnType>(script).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0] {
            Record::Let { variables, sql, .. } => {
                assert_eq!(variables, &["id".to_string()]);
                assert_eq!(sql, "SELECT 1");
            }
            _ => panic!("expected Let record"),
        }
    }

    #[test]
    fn test_let_parsing_multiple_vars() {
        let script = "\
let id, name, value
SELECT 1, 'hello', 42
";
        let records = parse::<DefaultColumnType>(script).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0] {
            Record::Let { variables, sql, .. } => {
                assert_eq!(
                    variables,
                    &["id".to_string(), "name".to_string(), "value".to_string()]
                );
                assert_eq!(sql, "SELECT 1, 'hello', 42");
            }
            _ => panic!("expected Let record"),
        }
    }

    #[test]
    fn test_let_parsing_roundtrip() {
        let script = "\
let id
SELECT 1
";
        let records = parse::<DefaultColumnType>(script).unwrap();
        let unparsed = records[0].to_string();
        let reparsed = parse::<DefaultColumnType>(&unparsed).unwrap();
        assert_eq!(records.len(), reparsed.len());
        match (&records[0], &reparsed[0]) {
            (
                Record::Let {
                    variables: v1,
                    sql: s1,
                    ..
                },
                Record::Let {
                    variables: v2,
                    sql: s2,
                    ..
                },
            ) => {
                assert_eq!(v1, v2);
                assert_eq!(s1, s2);
            }
            _ => panic!("expected Let records"),
        }
    }

    #[test]
    fn test_let_parsing_error_empty_vars() {
        let script = "\
let
SELECT 1
";
        let err = parse::<DefaultColumnType>(script).unwrap_err();
        assert!(matches!(err.kind(), ParseErrorKind::InvalidLine(_)));
    }

    #[test]
    fn test_let_parsing_error_invalid_var_name() {
        let script = "\
let 123invalid
SELECT 1
";
        let err = parse::<DefaultColumnType>(script).unwrap_err();
        assert!(matches!(err.kind(), ParseErrorKind::InvalidVariableName(_)));
    }

    #[test]
    fn test_is_valid_variable_name() {
        assert!(is_valid_variable_name("foo"));
        assert!(is_valid_variable_name("_bar"));
        assert!(is_valid_variable_name("foo123"));
        assert!(is_valid_variable_name("__TEST_DIR__"));
        assert!(!is_valid_variable_name(""));
        assert!(!is_valid_variable_name("123"));
        assert!(!is_valid_variable_name("foo-bar"));
        assert!(!is_valid_variable_name("foo bar"));
    }
}
