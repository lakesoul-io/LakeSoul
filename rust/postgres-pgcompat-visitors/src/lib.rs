// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Pure SQL-AST rewrite helpers for PostgreSQL compatibility hooks.
//!
//! These helpers own the sqlparser `Visit`/`VisitMut` implementations used by
//! `postgres-lakesoul`.  They live in a separate crate so the large set of
//! generated `visit::<T>` monomorphisations is compiled here, in a small crate
//! with a stable sccache key, instead of being re-instantiated every time the
//! server crate changes.  Every public entry point is a plain synchronous
//! function whose signature never exposes a visitor type, so callers do not
//! re-monomorphise the visitor chain.

use std::collections::HashSet;
use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    DataType as SqlDataType, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, Ident, LimitClause, ObjectName, Query,
    SetExpr, Statement, TableFactor, Value, ValueWithSpan, Visit, VisitMut, Visitor,
    VisitorMut,
};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

/// Rewrite pgjdbc/PG wire bytea literals into `decode('...', 'hex')` calls.
pub fn rewrite_bytea_literals(sql: &str) -> PgWireResult<Option<String>> {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("bytea")
        || (!lower.contains("::bytea") && !lower.contains("as bytea"))
    {
        return Ok(None);
    }
    let statements = match datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser::new().parse(sql) {
        Ok(statements) => statements,
        Err(_) => return Ok(None),
    };
    let mut changed = false;
    let mut rewritten = Vec::with_capacity(statements.len());
    for mut statement in statements {
        let mut visitor = ByteaLiteralVisitor { changed: false };
        if let ControlFlow::Break(error) = VisitMut::visit(&mut statement, &mut visitor) {
            return Err(error);
        }
        changed |= visitor.changed;
        rewritten.push(statement.to_string());
    }
    if changed {
        Ok(Some(rewritten.join("; ")))
    } else {
        Ok(None)
    }
}

struct ByteaLiteralVisitor {
    changed: bool,
}

impl VisitorMut for ByteaLiteralVisitor {
    type Break = PgWireError;

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Cast {
            expr: inner,
            data_type: SqlDataType::Bytea,
            ..
        } = expr
        else {
            return ControlFlow::Continue(());
        };
        let Some((text, escaped)) = string_literal(inner) else {
            return ControlFlow::Continue(());
        };
        let text = text.to_string();
        match decode_bytea_literal(&text, escaped) {
            Ok(bytes) => {
                *expr = bytea_decode_expr(&bytes);
                self.changed = true;
                ControlFlow::Continue(())
            }
            Err(()) => ControlFlow::Break(invalid_bytea_literal(&text)),
        }
    }
}

fn string_literal(expr: &Expr) -> Option<(&str, bool)> {
    match expr {
        Expr::Nested(inner) => string_literal(inner),
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(text) => Some((text, false)),
            Value::EscapedStringLiteral(text) => Some((text, true)),
            _ => None,
        },
        _ => None,
    }
}

/// PostgreSQL `byteain` for a literal's text. `escaped` distinguishes an
/// `E'...'` literal, whose backslash sequences are first interpreted by the
/// string literal parser, from a standard single-quoted literal.
fn decode_bytea_literal(text: &str, escaped: bool) -> Result<Vec<u8>, ()> {
    let text = if escaped {
        unescape_sql_literal(text)
    } else {
        text.to_string()
    };
    if let Some(hex) = text.strip_prefix("\\x") {
        if !hex.len().is_multiple_of(2) {
            return Err(());
        }
        let mut bytes = Vec::with_capacity(hex.len() / 2);
        let encoded = hex.as_bytes();
        for pair in encoded.chunks(2) {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            bytes.push(high << 4 | low);
        }
        return Ok(bytes);
    }

    let mut bytes = Vec::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            let mut buffer = [0; 4];
            bytes.extend_from_slice(ch.encode_utf8(&mut buffer).as_bytes());
            continue;
        }
        match chars.next() {
            Some('\\') => bytes.push(b'\\'),
            Some(first) if first.is_digit(8) => {
                let mut value = first.to_digit(8).ok_or(())?;
                for _ in 0..2 {
                    let digit = chars.next().and_then(|ch| ch.to_digit(8)).ok_or(())?;
                    value = value * 8 + digit;
                }
                bytes.push(u8::try_from(value).map_err(|_| ())?);
            }
            _ => return Err(()),
        }
    }
    Ok(bytes)
}

fn hex_digit(byte: u8) -> Result<u8, ()> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(()),
    }
}

fn unescape_sql_literal(text: &str) -> String {
    let mut unescaped = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            unescaped.push(ch);
            continue;
        }
        match chars.next() {
            Some('\\') => unescaped.push('\\'),
            Some('\'') => unescaped.push('\''),
            Some('n') => unescaped.push('\n'),
            Some('r') => unescaped.push('\r'),
            Some('t') => unescaped.push('\t'),
            Some('b') => unescaped.push('\u{8}'),
            Some('f') => unescaped.push('\u{c}'),
            Some(other) => {
                unescaped.push('\\');
                unescaped.push(other);
            }
            None => unescaped.push('\\'),
        }
    }
    unescaped
}

fn bytea_decode_expr(bytes: &[u8]) -> Expr {
    let hex = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Expr::Function(Function {
        name: ObjectName::from(Ident::new("decode")),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString(hex).with_empty_span(),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString("hex".to_string()).with_empty_span(),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn invalid_bytea_literal(text: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "22P02".to_string(),
        format!("invalid input syntax for type bytea: \"{text}\""),
    )))
}

/// Rewrites pgjdbc's primary-key metadata query.
///
/// pgjdbc calls `information_schema._pg_expandarray(i.indkey)` to turn the
/// index key vector into one row per key column. That set-returning function
/// is not implemented here, so the query never reaches the catalog scan.
/// LakeSoul's catalog keeps the primary-key columns in the table provider, so
/// the hook answers pgjdbc's outer six-column projection directly from those

/// Whether a statement is a catalog-projection query needing rewrite.
pub fn is_catalog_projection_query(statement: &Statement) -> bool {
    is_catalog_projection_query_impl(statement)
}

/// Rewrite catalog-projection subqueries (limit/strip correlation).
pub fn rewrite_catalog_projection(statement: &Statement) -> Statement {
    rewrite_catalog_projection_impl(statement)
}

pub struct CatalogProjectionHook;

fn is_catalog_projection_query_impl(statement: &Statement) -> bool {
    if !matches!(statement, Statement::Query(_)) {
        return false;
    }
    let lower = statement.to_string().to_ascii_lowercase();
    lower.contains("pg_attribute")
        && (lower.contains("format_type") || lower.contains("json_build_object"))
}

fn rewrite_catalog_projection_impl(statement: &Statement) -> Statement {
    let mut statement = statement.clone();
    let mut visitor = CatalogProjectionVisitor;
    let _ = VisitMut::visit(&mut statement, &mut visitor);
    statement
}

struct CatalogProjectionVisitor;

impl CatalogProjectionVisitor {
    fn has_correlation(&self, query: &Query) -> bool {
        if let SetExpr::Select(select) = &*query.body {
            let table_aliases: HashSet<String> = select
                .from
                .iter()
                .flat_map(|twj| {
                    let mut aliases = HashSet::new();
                    Self::collect_table_aliases_from_table_factor(
                        &twj.relation,
                        &mut aliases,
                    );
                    for join in &twj.joins {
                        Self::collect_table_aliases_from_table_factor(
                            &join.relation,
                            &mut aliases,
                        );
                    }
                    aliases
                })
                .collect();

            let mut has_correlation = false;
            let mut visitor =
                CorrelationCheckVisitor(&mut has_correlation, &table_aliases);
            let _ = Visit::visit(query, &mut visitor);
            has_correlation
        } else {
            false
        }
    }

    fn has_limit(&self, query: &Query) -> bool {
        query.limit_clause.is_some() || query.fetch.is_some()
    }

    fn collect_table_aliases_from_table_factor(
        table_factor: &TableFactor,
        aliases: &mut HashSet<String>,
    ) {
        if let TableFactor::Table {
            alias: Some(alias), ..
        } = table_factor
        {
            aliases.insert(alias.name.value.clone());
        }
    }
}

struct CorrelationCheckVisitor<'a>(&'a mut bool, &'a HashSet<String>);

impl Visitor for CorrelationCheckVisitor<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Value(ValueWithSpan {
                value: Value::Placeholder(_),
                ..
            }) => {
                *self.0 = true;
            }
            Expr::CompoundIdentifier(idents)
                if !idents.is_empty() && !self.1.contains(&idents[0].value) =>
            {
                *self.0 = true;
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

impl VisitorMut for CatalogProjectionVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Subquery(subquery) = expr {
            if self.has_correlation(subquery) {
                *expr = Expr::Value(Value::Null.with_empty_span());
            } else if !self.has_limit(subquery) {
                subquery.limit_clause = Some(LimitClause::LimitOffset {
                    limit: Some(Expr::Value(
                        Value::Number("1".to_string(), false).with_empty_span(),
                    )),
                    offset: None,
                    limit_by: vec![],
                });
            }
        }
        ControlFlow::Continue(())
    }
}

/// Replace `pg_settings` table factors with the derived `(VALUES ...)` table.
pub fn rewrite_pg_settings(
    statement: &Statement,
    derived: TableFactor,
) -> Option<Statement> {
    let mut visitor = SettingsTableVisitor {
        derived,
        replaced: false,
    };
    let mut statement = statement.clone();
    let _ = VisitMut::visit(&mut statement, &mut visitor);
    visitor.replaced.then_some(statement)
}

struct SettingsTableVisitor {
    derived: TableFactor,
    replaced: bool,
}

impl VisitorMut for SettingsTableVisitor {
    type Break = ();

    fn pre_visit_table_factor(
        &mut self,
        factor: &mut TableFactor,
    ) -> std::ops::ControlFlow<Self::Break> {
        let TableFactor::Table { name, alias, .. } = factor else {
            return std::ops::ControlFlow::Continue(());
        };
        let Some(last) = name.0.last().and_then(|part| part.as_ident()) else {
            return std::ops::ControlFlow::Continue(());
        };
        if !last.value.eq_ignore_ascii_case("pg_settings") {
            return std::ops::ControlFlow::Continue(());
        }
        let mut derived = self.derived.clone();
        if let TableFactor::Derived {
            alias: derived_alias,
            ..
        } = &mut derived
            && let Some(alias) = alias
            && let Some(derived_alias) = derived_alias
        {
            derived_alias.name = alias.name.clone();
        }
        *factor = derived;
        self.replaced = true;
        std::ops::ControlFlow::Continue(())
    }
}

/// Whether a statement references the virtual `pg_cursors` relation.
pub fn references_pg_cursors(statement: &Statement) -> bool {
    references_pg_cursors_impl(statement)
}

fn references_pg_cursors_impl(statement: &Statement) -> bool {
    if !matches!(statement, Statement::Query(_)) {
        return false;
    }
    struct Detector(bool);
    impl Visitor for Detector {
        type Break = ();
        fn pre_visit_table_factor(
            &mut self,
            table_factor: &TableFactor,
        ) -> ControlFlow<Self::Break> {
            if let TableFactor::Table { name, .. } = table_factor
                && name
                    .0
                    .last()
                    .and_then(|part| part.as_ident())
                    .is_some_and(|ident| ident.value.eq_ignore_ascii_case("pg_cursors"))
            {
                self.0 = true;
            }
            ControlFlow::Continue(())
        }
    }
    let mut detector = Detector(false);
    let _ = Visit::visit(statement, &mut detector);
    detector.0
}
