use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::ops::ControlFlow;

use crate::pg_catalog::PG_CATALOG_TABLES;

use datafusion::sql::sqlparser::ast::Array;
use datafusion::sql::sqlparser::ast::ArrayElemTypeDef;
use datafusion::sql::sqlparser::ast::BinaryOperator;
use datafusion::sql::sqlparser::ast::CastKind;
use datafusion::sql::sqlparser::ast::DataType;
use datafusion::sql::sqlparser::ast::Expr;
use datafusion::sql::sqlparser::ast::Function;
use datafusion::sql::sqlparser::ast::FunctionArg;
use datafusion::sql::sqlparser::ast::FunctionArgExpr;
use datafusion::sql::sqlparser::ast::FunctionArgumentList;
use datafusion::sql::sqlparser::ast::FunctionArguments;
use datafusion::sql::sqlparser::ast::Ident;
use datafusion::sql::sqlparser::ast::ObjectName;
use datafusion::sql::sqlparser::ast::ObjectNamePart;
use datafusion::sql::sqlparser::ast::OrderByKind;
use datafusion::sql::sqlparser::ast::Query;
use datafusion::sql::sqlparser::ast::Select;
use datafusion::sql::sqlparser::ast::SelectItem;
use datafusion::sql::sqlparser::ast::SelectItemQualifiedWildcardKind;
use datafusion::sql::sqlparser::ast::SetExpr;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::ast::TableFactor;
use datafusion::sql::sqlparser::ast::TableWithJoins;
use datafusion::sql::sqlparser::ast::UnaryOperator;
use datafusion::sql::sqlparser::ast::Value;
use datafusion::sql::sqlparser::ast::ValueWithSpan;
use datafusion::sql::sqlparser::ast::VisitMut;
use datafusion::sql::sqlparser::ast::VisitorMut;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;

pub trait SqlStatementRewriteRule: Send + Sync + Debug {
    fn rewrite(&self, s: Statement) -> Statement;
}

/// Rewrite rule for adding alias to duplicated projection
///
/// This rule is to deal with sql like `SELECT n.oid, n.* FROM n`, which is a
/// valid statement in postgres. But datafusion treat it as illegal because of
/// duplicated column oid in projection.
///
/// This rule will add alias to column, when there is a wildcard found in
/// projection.
#[derive(Debug)]
pub struct AliasDuplicatedProjectionRewrite;

impl AliasDuplicatedProjectionRewrite {
    // Rewrites a SELECT statement to alias explicit columns from the same table as a qualified wildcard.
    fn rewrite_select_with_alias(select: &mut Box<Select>) {
        // 1. Collect all table aliases from qualified wildcards.
        let mut wildcard_tables = Vec::new();
        let mut has_simple_wildcard = false;
        for p in &select.projection {
            match p {
                SelectItem::QualifiedWildcard(name, _) => match name {
                    SelectItemQualifiedWildcardKind::ObjectName(objname) => {
                        // for n.oid,
                        let idents = objname
                            .0
                            .iter()
                            .map(|v| v.as_ident().unwrap().value.clone())
                            .collect::<Vec<_>>()
                            .join(".");

                        wildcard_tables.push(idents);
                    }
                    SelectItemQualifiedWildcardKind::Expr(_expr) => {
                        // FIXME:
                    }
                },
                SelectItem::Wildcard(_) => {
                    has_simple_wildcard = true;
                }
                _ => {}
            }
        }

        // If there are no qualified wildcards, there's nothing to do.
        if wildcard_tables.is_empty() && !has_simple_wildcard {
            return;
        }

        // 2. Rewrite the projection, adding aliases to matching columns.
        let mut new_projection = vec![];
        for p in select.projection.drain(..) {
            match p {
                SelectItem::UnnamedExpr(expr) => {
                    let alias_partial = match &expr {
                        // Case for `oid` (unqualified identifier)
                        Expr::Identifier(ident) => Some(ident.clone()),
                        // Case for `n.oid` (compound identifier)
                        Expr::CompoundIdentifier(idents) => {
                            // compare every ident but the last
                            if idents.len() > 1 {
                                let table_name = &idents[..idents.len() - 1]
                                    .iter()
                                    .map(|i| i.value.clone())
                                    .collect::<Vec<_>>()
                                    .join(".");
                                if wildcard_tables.iter().any(|name| name == table_name) {
                                    Some(idents[idents.len() - 1].clone())
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };

                    if let Some(name) = alias_partial {
                        let alias = format!("__alias_{name}");
                        new_projection.push(SelectItem::ExprWithAlias {
                            expr,
                            alias: Ident::new(alias),
                        });
                    } else {
                        new_projection.push(SelectItem::UnnamedExpr(expr));
                    }
                }
                // Preserve existing aliases and wildcards.
                _ => new_projection.push(p),
            }
        }
        select.projection = new_projection;
    }
}

impl SqlStatementRewriteRule for AliasDuplicatedProjectionRewrite {
    fn rewrite(&self, mut statement: Statement) -> Statement {
        if let Statement::Query(query) = &mut statement
            && let SetExpr::Select(select) = query.body.as_mut()
        {
            Self::rewrite_select_with_alias(select);
        }

        statement
    }
}

/// Prepend qualifier for order by or filter when there is qualified wildcard
///
/// Postgres allows unqualified identifier in ORDER BY and FILTER but it's not
/// accepted by datafusion.
#[derive(Debug)]
pub struct ResolveUnqualifiedIdentifier;

impl ResolveUnqualifiedIdentifier {
    fn rewrite_unqualified_identifiers(query: &mut Box<Query>) {
        if let SetExpr::Select(select) = query.body.as_mut() {
            // Step 1: Find all table aliases from FROM and JOIN clauses.
            let table_aliases = Self::get_table_aliases(&select.from);

            // Step 2: Check for a single qualified wildcard in the projection.
            let qualified_wildcard_alias = Self::get_qualified_wildcard_alias(&select.projection);
            if qualified_wildcard_alias.is_none() || table_aliases.is_empty() {
                return; // Conditions not met.
            }

            let wildcard_alias = qualified_wildcard_alias.unwrap();

            // Step 2.5: Collect all projection aliases to avoid rewriting them
            let projection_aliases = Self::get_projection_aliases(&select.projection);

            // Step 3: Rewrite expressions in the WHERE and ORDER BY clauses.
            if let Some(selection) = &mut select.selection {
                Self::rewrite_expr(
                    selection,
                    &wildcard_alias,
                    &table_aliases,
                    &projection_aliases,
                );
            }

            if let Some(OrderByKind::Expressions(order_by_exprs)) =
                query.order_by.as_mut().map(|o| &mut o.kind)
            {
                for order_by_expr in order_by_exprs {
                    Self::rewrite_expr(
                        &mut order_by_expr.expr,
                        &wildcard_alias,
                        &table_aliases,
                        &projection_aliases,
                    );
                }
            }
        }
    }

    fn get_table_aliases(tables: &[TableWithJoins]) -> HashSet<String> {
        let mut aliases = HashSet::new();
        for table_with_joins in tables {
            if let TableFactor::Table {
                alias: Some(alias), ..
            } = &table_with_joins.relation
            {
                aliases.insert(alias.name.value.clone());
            }
            for join in &table_with_joins.joins {
                if let TableFactor::Table {
                    alias: Some(alias), ..
                } = &join.relation
                {
                    aliases.insert(alias.name.value.clone());
                }
            }
        }
        aliases
    }

    fn get_qualified_wildcard_alias(projection: &[SelectItem]) -> Option<String> {
        let mut qualified_wildcards = projection
            .iter()
            .filter_map(|item| {
                if let SelectItem::QualifiedWildcard(
                    SelectItemQualifiedWildcardKind::ObjectName(objname),
                    _,
                ) = item
                {
                    Some(
                        objname
                            .0
                            .iter()
                            .map(|v| v.as_ident().unwrap().value.clone())
                            .collect::<Vec<_>>()
                            .join("."),
                    )
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if qualified_wildcards.len() == 1 {
            Some(qualified_wildcards.remove(0))
        } else {
            None
        }
    }

    fn get_projection_aliases(projection: &[SelectItem]) -> HashSet<String> {
        let mut aliases = HashSet::new();
        for item in projection {
            match item {
                SelectItem::ExprWithAlias { alias, .. } => {
                    aliases.insert(alias.value.clone());
                }
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    aliases.insert(ident.value.clone());
                }
                _ => {}
            }
        }
        aliases
    }

    fn rewrite_expr(
        expr: &mut Expr,
        wildcard_alias: &str,
        table_aliases: &HashSet<String>,
        projection_aliases: &HashSet<String>,
    ) {
        match expr {
            // If the identifier is not a table alias itself and not already aliased in projection, rewrite it.
            Expr::Identifier(ident)
                if !table_aliases.contains(&ident.value)
                    && !projection_aliases.contains(&ident.value) =>
            {
                *expr = Expr::CompoundIdentifier(vec![
                    Ident::new(wildcard_alias.to_string()),
                    ident.clone(),
                ]);
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::rewrite_expr(left, wildcard_alias, table_aliases, projection_aliases);
                Self::rewrite_expr(right, wildcard_alias, table_aliases, projection_aliases);
            }
            // Add more cases for other expression types as needed (e.g., `InList`, `Between`, etc.)
            _ => {}
        }
    }
}

impl SqlStatementRewriteRule for ResolveUnqualifiedIdentifier {
    fn rewrite(&self, mut statement: Statement) -> Statement {
        if let Statement::Query(query) = &mut statement {
            Self::rewrite_unqualified_identifiers(query);
        }

        statement
    }
}

/// Rewrite the Postgres `ANY`/`ALL` quantified comparison operators over
/// arrays into DataFusion's `array_contains` function.
///
/// Postgres `x <op> ANY (arr)` / `x <op> ALL (arr)` have no direct DataFusion
/// equivalent, so this rule lowers the two cases that map cleanly onto
/// membership:
///
/// * `x =  ANY(arr)`  -> `array_contains(arr, x)`  (x equals some element)
/// * `x <> ALL(arr)`  -> `NOT array_contains(arr, x)` (x differs from every
///   element, i.e. is not a member)
///
/// The other two combinations are **not** handled, because `array_contains`
/// only expresses membership, not universal quantification:
///
/// * `x <> ANY(arr)` (x differs from *some* element) is true for essentially
///   every non-empty array and has no faithful lowering to membership.
/// * `x =  ALL(arr)` (x equals *every* element) needs "all elements are x",
///   which membership cannot express without an additional length/count check.
///
/// These are left untouched; if a client query uses them, DataFusion reports
/// the unsupported operator rather than silently producing wrong results.
///
/// # String-literal array form
///
/// Postgres allows an array as a single-quoted string literal, e.g.
/// `x = ANY('{r, l, e}')`. When the right operand is such a literal this rule
/// parses it into an `ARRAY[...]` expression first. The parsing is **naive**:
/// it splits on commas and treats every element as a string. That is correct
/// for the string-array case clients actually send (privilege/role char arrays,
/// `contyp`/`stxkind` style columns), but it does not handle quoted elements
/// containing commas, and it assumes string element types for non-string array
/// literals. A non-string-literal right operand (a column, `ARRAY[...]`, or a
/// function call) is passed through unchanged.
#[derive(Debug)]
pub struct RewriteArrayAnyAllOperation;

struct RewriteArrayAnyAllOperationVisitor;

impl RewriteArrayAnyAllOperationVisitor {
    fn any_to_array_contains(&self, left: &Expr, right: &Expr) -> Expr {
        let array = if let Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(array_literal),
            ..
        }) = right
        {
            let array_literal = array_literal.trim();
            if array_literal.starts_with('{') && array_literal.ends_with('}') {
                let items = array_literal.trim_matches(|c| c == '{' || c == '}' || c == ' ');
                let items = items.split(',').map(|s| s.trim()).filter(|s| !s.is_empty());

                // Elements are treated as strings -- see the rule-level doc
                // comment for why this naive split is sufficient for the array
                // literals clients actually send (string-typed arrays) but does
                // not handle quoted elements containing commas.
                let elems = items
                    .map(|s| {
                        Expr::Value(Value::SingleQuotedString(s.to_string()).with_empty_span())
                    })
                    .collect();
                Expr::Array(Array {
                    elem: elems,
                    named: true,
                })
            } else {
                right.clone()
            }
        } else {
            right.clone()
        };

        Expr::Function(Function {
            name: ObjectName::from(vec![Ident::new("array_contains")]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(array)),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(left.clone())),
                ],
                duplicate_treatment: None,
                clauses: vec![],
            }),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        })
    }
}

impl VisitorMut for RewriteArrayAnyAllOperationVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } => match compare_op {
                BinaryOperator::Eq => {
                    *expr = self.any_to_array_contains(left.as_ref(), right.as_ref());
                }
                BinaryOperator::NotEq => {
                    // x <> ANY(arr): true if x differs from SOME element. That
                    // is true for almost every non-empty array and cannot be
                    // lowered to `array_contains`; leave it for DataFusion to
                    // report as unsupported.
                }
                _ => {}
            },
            Expr::AllOp {
                left,
                compare_op,
                right,
            } => match compare_op {
                BinaryOperator::Eq => {
                    // x = ALL(arr): true if EVERY element equals x. Membership
                    // alone cannot express this; leave it for DataFusion to
                    // report as unsupported.
                }
                BinaryOperator::NotEq => {
                    *expr = Expr::UnaryOp {
                        op: UnaryOperator::Not,
                        expr: Box::new(self.any_to_array_contains(left.as_ref(), right.as_ref())),
                    }
                }
                _ => {}
            },
            _ => {}
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for RewriteArrayAnyAllOperation {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = RewriteArrayAnyAllOperationVisitor;

        let _ = s.visit(&mut visitor);

        s
    }
}

/// Prepend qualifier to table_name
///
/// Postgres has pg_catalog in search_path by default so it allow access to
/// `pg_namespace` without `pg_catalog.` qualifier.
///
/// Only names of known pg_catalog relations are qualified. A simple `pg_`
/// prefix check would also rewrite user tables such as `pg_compat_test`.
#[derive(Debug)]
pub struct PrependUnqualifiedPgTableName;

fn is_pg_catalog_table(name: &str) -> bool {
    PG_CATALOG_TABLES
        .iter()
        .any(|table| table.eq_ignore_ascii_case(name))
}

struct PrependUnqualifiedPgTableNameVisitor;

impl VisitorMut for PrependUnqualifiedPgTableNameVisitor {
    type Break = ();

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let TableFactor::Table { name, args, .. } = table_factor {
            // not a table function
            if args.is_none()
                && name.0.len() == 1
                && let ObjectNamePart::Identifier(ident) = &name.0[0]
                && is_pg_catalog_table(&ident.value)
            {
                *name = ObjectName(vec![
                    ObjectNamePart::Identifier(Ident::new("pg_catalog")),
                    name.0[0].clone(),
                ]);
            }
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for PrependUnqualifiedPgTableName {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = PrependUnqualifiedPgTableNameVisitor;

        let _ = s.visit(&mut visitor);
        s
    }
}

#[derive(Debug)]
pub struct FixArrayLiteral;

struct FixArrayLiteralVisitor;

impl FixArrayLiteralVisitor {
    fn is_string_type(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::Text | DataType::Varchar(_) | DataType::Char(_) | DataType::String(_)
        )
    }
}

impl VisitorMut for FixArrayLiteralVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Cast {
            kind,
            expr,
            data_type,
            ..
        } = expr
            && kind == &CastKind::DoubleColon
            && let DataType::Array(arr) = data_type
        {
            // cast some to
            if let Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(array_literal),
                ..
            }) = expr.as_ref()
            {
                let items = array_literal.trim_matches(|c| c == '{' || c == '}' || c == ' ');
                let items = items.split(',').map(|s| s.trim()).filter(|s| !s.is_empty());

                let is_text = match arr {
                    ArrayElemTypeDef::AngleBracket(dt) => Self::is_string_type(dt.as_ref()),
                    ArrayElemTypeDef::SquareBracket(dt, _) => Self::is_string_type(dt.as_ref()),
                    ArrayElemTypeDef::Parenthesis(dt) => Self::is_string_type(dt.as_ref()),
                    _ => false,
                };

                let elems = items
                    .map(|s| {
                        if is_text {
                            Expr::Value(Value::SingleQuotedString(s.to_string()).with_empty_span())
                        } else {
                            Expr::Value(Value::Number(s.to_string(), false).with_empty_span())
                        }
                    })
                    .collect();
                **expr = Expr::Array(Array {
                    elem: elems,
                    named: true,
                });
            }
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for FixArrayLiteral {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = FixArrayLiteralVisitor;

        let _ = s.visit(&mut visitor);
        s
    }
}

/// Strip the schema qualifier from callable names.
///
/// DataFusion resolves functions (including table functions) by bare name in
/// a flat namespace -- it has no notion of a schema-qualified function path.
/// Postgres clients routinely write the qualifier explicitly, most often
/// `pg_catalog.` (e.g. `pg_catalog.array_to_string(...)`,
/// `pg_catalog.pg_get_keywords()`), so this rule drops the leading
/// identifier(s) from any multi-part function or table-function name and
/// keeps only the last segment.
///
/// Type-cast qualifiers such as `pg_catalog.text` / `pg_catalog.int2[]` used
/// to be handled here too, but are now resolved by the [`PgOidTypePlanner`]
/// type planner (which sees the cast target type at planning time and maps the
/// pg name to its Arrow type).
///
/// [`PgOidTypePlanner`]: crate::pg_catalog::oid_type_planner::PgOidTypePlanner
#[derive(Debug)]
pub struct StripCallableQualifier;

struct StripCallableQualifierVisitor;

impl VisitorMut for StripCallableQualifierVisitor {
    type Break = ();

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        // Strip qualifier from table function names (any schema, not just
        // pg_catalog): DF resolves table functions by bare name.
        if let TableFactor::Table { name, args, .. } = table_factor
            && args.is_some()
        {
            // multiple idents in name means it's a qualified table name
            if name.0.len() > 1
                && let Some(last_ident) = name.0.pop()
            {
                *name = ObjectName(vec![last_ident]);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        // Strip qualifier from function names (any schema, not just
        // pg_catalog): DF resolves functions by bare name.
        if let Expr::Function(function) = expr {
            let name = &mut function.name;
            if name.0.len() > 1
                && let Some(last_ident) = name.0.pop()
            {
                *name = ObjectName(vec![last_ident]);
            }
        }
        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for StripCallableQualifier {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = StripCallableQualifierVisitor;

        let _ = s.visit(&mut visitor);
        s
    }
}

/// Rewrite postgres built-in special registers that may be referenced
/// without parentheses into the function-call form DataFusion understands.
///
/// Postgres allows several built-in special registers to be used without
/// parentheses -- e.g. `current_user`, `current_catalog`, `current_schema`.
/// DataFusion only exposes these as function calls, so this rule rewrites the
/// parenthesis-less forms. sqlparser represents them in two different ways,
/// so the rule handles both:
///
///   * `current_user` and `current_catalog` are sqlparser-special-cased
///     keywords that already parse as a zero-argument `Function` (with
///     `FunctionArguments::None`). They only need renaming, because DataFusion
///     exposes them under different names: `current_user` -> `session_user`,
///     `current_catalog` -> `current_database`.
///   * `current_schema` parses as a bare identifier and is wrapped into a
///     zero-argument function call: `current_schema` -> `current_schema()`.
///
/// Because the rewritten AST is re-serialised and re-parsed by DataFusion
/// (see `handlers.rs`), the emitted form must round-trip through the postgres
/// dialect. sqlparser only accepts the parenthesis-less spelling for the
/// special-cased keywords [`PARENTHESIS_LESS_KEYWORDS`]; every other target
/// (e.g. `current_database`) must be emitted with explicit `()`.
///
/// Note: `version` is deliberately NOT handled here -- in postgres it is a
/// regular function, not a special register, so `SELECT version` (no parens)
/// is invalid and never reaches this rule.
#[derive(Debug)]
pub struct CurrentUserVariableToSessionUserFunctionCall;

/// `(postgres function name, target function name)` for the parenthesis-less
/// `Function` nodes produced by sqlparser that need renaming.
const BUILTIN_FUNCTION_RENAME: &[(&str, &str)] = &[
    ("current_user", "session_user"),
    ("current_catalog", "current_database"),
];

/// sqlparser special-cases these keywords into a parenthesis-less zero-argument
/// `Function` (see sqlparser's `Parser::parse_word`). They -- and only they --
/// may be emitted without parentheses; `name()` does not re-parse for them, so
/// any other target must be serialised as an explicit `name()` call.
const PARENTHESIS_LESS_KEYWORDS: &[&str] =
    &["current_catalog", "current_user", "session_user", "user"];

/// `(identifier, target function name)` for bare identifiers that must be
/// wrapped into a zero-argument function call.
const BARE_IDENTIFIER_TO_FUNCTION: &[(&str, &str)] = &[("current_schema", "current_schema")];

fn empty_function_call(name: &str) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(name)]),
        args: FunctionArguments::List(FunctionArgumentList {
            args: vec![],
            duplicate_treatment: None,
            clauses: vec![],
        }),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

struct CurrentUserVariableToSessionUserFunctionCallVisitor;

impl CurrentUserVariableToSessionUserFunctionCallVisitor {
    fn last_ident_lower(name: &ObjectName) -> Option<String> {
        name.0
            .last()
            .and_then(|part| part.as_ident())
            .map(|ident| ident.value.to_lowercase())
    }
}

impl VisitorMut for CurrentUserVariableToSessionUserFunctionCallVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        // Bare identifier -> zero-argument function call.
        if let Expr::Identifier(ident) = expr
            && ident.quote_style.is_none()
        {
            let lower = ident.value.to_lowercase();
            if let Some(&(_, target)) = BARE_IDENTIFIER_TO_FUNCTION
                .iter()
                .find(|(name, _)| *name == lower)
            {
                *expr = empty_function_call(target);
            }
        }

        // Parenthesis-less `Function` (or explicit call) -> rename where needed.
        // A parenthesis-less `FunctionArguments::None` is only kept as-is when
        // the *target* is itself a parenthesis-less special keyword; otherwise
        // it is normalised to an empty argument list so the rewritten SQL
        // re-parses as a real `name()` call (the parenthesis-less spelling does
        // not round-trip for non-special names such as `current_database`).
        if let Expr::Function(func) = expr
            && let Some(fname) = Self::last_ident_lower(&func.name)
            && let Some(&(_, target)) = BUILTIN_FUNCTION_RENAME
                .iter()
                .find(|(name, _)| *name == fname)
        {
            func.name = ObjectName::from(vec![Ident::new(target)]);
            if matches!(func.args, FunctionArguments::None)
                && !PARENTHESIS_LESS_KEYWORDS.contains(&target)
            {
                func.args = FunctionArguments::List(FunctionArgumentList {
                    args: vec![],
                    duplicate_treatment: None,
                    clauses: vec![],
                });
            }
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for CurrentUserVariableToSessionUserFunctionCall {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = CurrentUserVariableToSessionUserFunctionCallVisitor;

        let _ = s.visit(&mut visitor);
        s
    }
}

/// Strip Postgres `COLLATE` clauses.
///
/// Postgres allows trailing `COLLATE <schema>.<collation>` on expressions
/// (e.g. `c.relname COLLATE pg_catalog.default`). DataFusion has no notion of
/// collation and rejects this syntax at parse time, so strip the clause and
/// keep the inner expression. The collation is irrelevant to DataFusion's
/// byte-wise string comparison anyway.
#[derive(Debug)]
pub struct StripCollate;

struct StripCollateVisitor;

impl VisitorMut for StripCollateVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Collate { expr: inner, .. } = expr {
            *expr = inner.as_ref().clone();
        }
        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for StripCollate {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = StripCollateVisitor;
        let _ = s.visit(&mut visitor);
        s
    }
}

/// Rewrite Postgres `OPERATOR(schema.name)` call syntax to the bare operator.
///
/// Postgres lets clients force an operator's schema with
/// `lhs OPERATOR(pg_catalog.~) rhs`. sqlparser represents this as a
/// [`BinaryOperator::PGCustomBinaryOperator`]; DataFusion only understands the
/// built-in operator variants, so map every `pg_catalog.<regex-op>` we see
/// back to its builtin form:
///
/// | pg_catalog name | builtin operator            |
/// | --------------- | --------------------------- |
/// | `~`             | `PGRegexMatch`    (`~`)     |
/// | `~*`            | `PGRegexIMatch`   (`~*`)    |
/// | `!~`            | `PGRegexNotMatch` (`!~`)    |
/// | `!~*`           | `PGRegexNotIMatch`(`!~*`)   |
///
/// Only the four regex operators are mapped, because those are the ones
/// clients emit via `OPERATOR(...)` in practice (psql's `\d` queries use
/// `OPERATOR(pg_catalog.~)`). Any other operator name is left untouched for
/// DataFusion to report.
#[derive(Debug)]
pub struct RewritePgCatalogOperator;

struct RewritePgCatalogOperatorVisitor;

impl VisitorMut for RewritePgCatalogOperatorVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::BinaryOp { op, .. } = expr
            && let BinaryOperator::PGCustomBinaryOperator(ops) = op
            && ops.first().is_some_and(|s| s == "pg_catalog")
            && let Some(name) = ops.last().map(String::as_str)
        {
            // Map every `OPERATOR(pg_catalog.<regex-op>)` to its builtin form.
            // Match on the operator name (last segment); the schema is checked
            // above. Only the four regex ops have a builtin equivalent
            // DataFusion understands, so any other name is left untouched.
            *op = match name {
                "~" => BinaryOperator::PGRegexMatch,
                "~*" => BinaryOperator::PGRegexIMatch,
                "!~" => BinaryOperator::PGRegexNotMatch,
                "!~*" => BinaryOperator::PGRegexNotIMatch,
                _ => return ControlFlow::Continue(()),
            };
        }
        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for RewritePgCatalogOperator {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = RewritePgCatalogOperatorVisitor;
        let _ = s.visit(&mut visitor);
        s
    }
}

/// Rewrite forward oid-alias casts (`'x'::regclass`, `'public'::regnamespace`,
/// ...) to oid values the way Postgres does.
///
/// Postgres oid-alias types cast a *name* to the matching catalog oid;
/// DataFusion has no such types. This rule rewrites the **forward** (name->oid)
/// direction at the AST layer, before DataFusion plans the query:
///
/// * a **numeric** string operand (`'16417'`) becomes a literal oid --
///   Postgres treats a numeric string as the oid itself, and emitting a literal
///   avoids embedding a `(SELECT ...)` in contexts DataFusion can't decorrelate
///   (e.g. `VALUES ('16417'::regclass)` inside `IN (...)`);
/// * a **name** string operand (`'public'`) becomes a scalar subquery against
///   the backing catalog table. The subquery is plain SQL, so DataFusion
///   resolves `pg_catalog.pg_namespace` / `pg_class` / `pg_type` / `pg_proc`
///   through the catalog at plan time -- **no `TableProvider` or catalog handle
///   is needed in the rule**, which is why this lives at the SQL layer rather
///   than as an analyzer rule.
///
/// Only the forward direction is rewritten (operand is a string literal or a
/// prepared-statement `$N` placeholder). Reverse / column-operand casts such
/// as `prorettype::regtype::text` (oid -> name for display) are left untouched;
/// the `PgOidTypePlanner` maps those cast types to `Int32` so they still parse.
///
/// # Limitation vs an analyzer rule
///
/// Because the SQL layer has no schema, this rule can only resolve casts that
/// *explicitly* name an oid-alias type. The **implicit** form
/// `oid_col = 'public'` (bare string vs an oid column) is not recognized here
/// -- there's no cast in the AST to key off. Resolving that would require
/// schema awareness (an analyzer rule + a catalog provider). In practice the
/// only client query affected is blacklisted.
#[derive(Debug)]
pub struct RewriteRegCastToSubquery {
    /// Pre-parsed `(kind -> query template)` map. Each template contains a
    /// single `$1` placeholder into which the cast operand is substituted.
    templates: HashMap<String, Box<Query>>,
}

/// One oid-alias type plus the name->oid lookup query that resolves it. The
/// query must contain exactly one `$1` placeholder, into which the cast
/// operand is substituted verbatim.
struct RegCastSpec {
    /// Bare lowercased type name, e.g. `"regclass"`. Both `regclass` and
    /// `pg_catalog.regclass` (and `REGCLASS`, which sqlparser uppercases) are
    /// matched against this after normalization.
    type_name: &'static str,
    query: &'static str,
}

const REG_CAST_SPECS: &[RegCastSpec] = &[
    // regclass: relation name -> pg_class.oid
    RegCastSpec {
        type_name: "regclass",
        query: "SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid CROSS JOIN (SELECT parse_ident($1::TEXT) AS parts) WHERE array_length(parts, 1) IN (1, 2) AND ((array_length(parts, 1) = 1 AND n.nspname = current_schema() AND c.relname = parts[1]) OR (array_length(parts, 1) = 2 AND n.nspname = parts[1] AND c.relname = parts[2]))",
    },
    // regnamespace: schema name -> pg_namespace.oid
    RegCastSpec {
        type_name: "regnamespace",
        query: "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = $1",
    },
    // regtype: type name -> pg_type.oid
    RegCastSpec {
        type_name: "regtype",
        query: "SELECT oid FROM pg_catalog.pg_type WHERE typname = $1",
    },
    // regproc: function name -> pg_proc.oid
    RegCastSpec {
        type_name: "regproc",
        query: "SELECT oid FROM pg_catalog.pg_proc WHERE proname = $1",
    },
];

impl Default for RewriteRegCastToSubquery {
    fn default() -> Self {
        Self::new()
    }
}

impl RewriteRegCastToSubquery {
    pub fn new() -> Self {
        let dialect = PostgreSqlDialect {};
        let mut templates = HashMap::new();
        for spec in REG_CAST_SPECS {
            let query = Parser::parse_sql(&dialect, spec.query)
                .map(|mut stmts| match stmts.remove(0) {
                    Statement::Query(query) => query,
                    _ => unreachable!("REG_CAST_SPECS entries are all queries"),
                })
                .expect("REG_CAST_SPECS entries must parse");
            templates.insert(spec.type_name.to_owned(), query);
        }
        Self { templates }
    }
}

struct RewriteRegCastToSubqueryVisitor<'a> {
    templates: &'a HashMap<String, Box<Query>>,
}

impl RewriteRegCastToSubqueryVisitor<'_> {
    /// Normalize a cast's `DataType` to the bare lowercased type name used as a
    /// key in [`REG_CAST_SPECS`]. sqlparser uppercases the dedicated
    /// `REGCLASS` keyword (`DataType::Regclass` -> `"REGCLASS"`) and renders
    /// schema-qualified types as `pg_catalog.regclass`, so lowercase and strip
    /// the optional `pg_catalog.` prefix uniformly.
    fn normalize_type_name(data_type: &DataType) -> String {
        let lower = data_type.to_string().to_lowercase();
        lower
            .strip_prefix("pg_catalog.")
            .unwrap_or(&lower)
            .to_owned()
    }

    /// True when `expr` is the forward (name->oid) cast operand we rewrite: a
    /// string literal or a prepared-statement placeholder. Column operands
    /// (the reverse oid->name direction) are deliberately excluded so a cast
    /// like `prorettype::regtype` is left for `PgOidTypePlanner`.
    fn is_name_operand(expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(_),
                ..
            }) | Expr::Value(ValueWithSpan {
                value: Value::Placeholder(_),
                ..
            })
        )
    }

    /// If `expr` is a single-quoted string whose contents are a base-10
    /// integer, return it as a SQL number literal. See the rule-level doc for
    /// why numeric strings resolve to a literal rather than a subquery.
    fn numeric_string_to_literal(expr: &Expr) -> Option<Expr> {
        if let Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        }) = expr
            && !s.is_empty()
            && s.bytes().all(|b| b.is_ascii_digit())
        {
            return Some(Expr::Value(
                Value::Number(s.clone(), false).with_empty_span(),
            ));
        }
        None
    }

    /// Build `(SELECT oid FROM pg_catalog.<table> WHERE <name_col> = <operand>)`
    /// by cloning the type's query template and substituting its `$1` with the
    /// cast operand.
    fn create_subquery(template: &Query, operand: &Expr) -> Expr {
        struct PlaceholderReplacer(Expr);
        impl VisitorMut for PlaceholderReplacer {
            type Break = ();
            fn pre_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<Self::Break> {
                if let Expr::Value(ValueWithSpan {
                    value: Value::Placeholder(_),
                    ..
                }) = e
                {
                    *e = self.0.clone();
                }
                ControlFlow::Continue(())
            }
        }

        let mut query = template.clone();
        let _ = query.visit(&mut PlaceholderReplacer(operand.clone()));
        Expr::Subquery(Box::new(query))
    }
}

impl VisitorMut for RewriteRegCastToSubqueryVisitor<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Cast {
            expr: operand,
            data_type,
            ..
        } = expr
            && Self::is_name_operand(operand)
            && let Some(template) = self.templates.get(&Self::normalize_type_name(data_type))
        {
            *expr = if let Some(lit) = Self::numeric_string_to_literal(operand) {
                lit
            } else {
                Self::create_subquery(template, operand)
            };
        }
        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for RewriteRegCastToSubquery {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = RewriteRegCastToSubqueryVisitor {
            templates: &self.templates,
        };
        let _ = s.visit(&mut visitor);
        s
    }
}

/// `select version()` should return column named `version` not `version()`
#[derive(Debug)]
pub struct FixVersionColumnName;

struct FixVersionColumnNameVisitor;

impl VisitorMut for FixVersionColumnNameVisitor {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let SetExpr::Select(select) = query.body.as_mut() {
            for projection in &mut select.projection {
                if let SelectItem::UnnamedExpr(Expr::Function(f)) = projection
                    && f.name.0.len() == 1
                    && let ObjectNamePart::Identifier(part) = &f.name.0[0]
                    && part.value == "version"
                    && let FunctionArguments::List(args) = &f.args
                    && args.args.is_empty()
                {
                    *projection = SelectItem::ExprWithAlias {
                        expr: Expr::Function(f.clone()),
                        alias: Ident::new("version"),
                    }
                }
            }
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for FixVersionColumnName {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = FixVersionColumnNameVisitor;
        let _ = s.visit(&mut visitor);

        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::{Parser, ParserError};
    use std::sync::Arc;

    fn parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
        let dialect = PostgreSqlDialect {};

        Parser::parse_sql(&dialect, sql)
    }

    fn rewrite(mut s: Statement, rules: &[Arc<dyn SqlStatementRewriteRule>]) -> Statement {
        for rule in rules {
            s = rule.rewrite(s);
        }

        s
    }

    macro_rules! assert_rewrite {
        ($rules:expr, $orig:expr, $rewt:expr) => {
            let sql = $orig;
            let statement = parse(sql).expect("Failed to parse").remove(0);

            let statement = rewrite(statement, $rules);
            assert_eq!(statement.to_string(), $rewt);
        };
    }

    #[test]
    fn test_alias_rewrite() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(AliasDuplicatedProjectionRewrite)];

        assert_rewrite!(
            &rules,
            "SELECT n.oid, n.* FROM pg_catalog.pg_namespace n",
            "SELECT n.oid AS __alias_oid, n.* FROM pg_catalog.pg_namespace n"
        );

        assert_rewrite!(
            &rules,
            "SELECT oid, * FROM pg_catalog.pg_namespace",
            "SELECT oid AS __alias_oid, * FROM pg_catalog.pg_namespace"
        );

        assert_rewrite!(
            &rules,
            "SELECT t1.oid, t2.* FROM tbl1 AS t1 JOIN tbl2 AS t2 ON t1.id = t2.id",
            "SELECT t1.oid, t2.* FROM tbl1 AS t1 JOIN tbl2 AS t2 ON t1.id = t2.id"
        );

        let sql = "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace' ORDER BY nspsname";
        let statement = parse(sql).expect("Failed to parse").remove(0);

        let statement = rewrite(statement, &rules);
        assert_eq!(
            statement.to_string(),
            "SELECT n.oid AS __alias_oid, n.*, d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid = n.oid AND d.objsubid = 0 AND d.classoid = 'pg_namespace' ORDER BY nspsname"
        );
    }

    #[test]
    fn test_qualifier_prepend() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(ResolveUnqualifiedIdentifier)];

        assert_rewrite!(
            &rules,
            "SELECT n.* FROM pg_catalog.pg_namespace n WHERE nspname = 'pg_catalog' ORDER BY nspname",
            "SELECT n.* FROM pg_catalog.pg_namespace n WHERE n.nspname = 'pg_catalog' ORDER BY n.nspname"
        );

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_catalog.pg_namespace ORDER BY nspname",
            "SELECT * FROM pg_catalog.pg_namespace ORDER BY nspname"
        );

        assert_rewrite!(
            &rules,
            "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace' ORDER BY nspsname",
            "SELECT n.oid, n.*, d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid = n.oid AND d.objsubid = 0 AND d.classoid = 'pg_namespace' ORDER BY n.nspsname"
        );

        assert_rewrite!(
            &rules,
            "SELECT i.*,i.indkey as keys,c.relname,c.relnamespace,c.relam,c.reltablespace,tc.relname as tabrelname,dsc.description FROM pg_catalog.pg_index i INNER JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid INNER JOIN pg_catalog.pg_class tc ON tc.oid=i.indrelid LEFT OUTER JOIN pg_catalog.pg_description dsc ON i.indexrelid=dsc.objoid WHERE i.indrelid=1 ORDER BY tabrelname, c.relname",
            "SELECT i.*, i.indkey AS keys, c.relname, c.relnamespace, c.relam, c.reltablespace, tc.relname AS tabrelname, dsc.description FROM pg_catalog.pg_index i INNER JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid INNER JOIN pg_catalog.pg_class tc ON tc.oid = i.indrelid LEFT OUTER JOIN pg_catalog.pg_description dsc ON i.indexrelid = dsc.objoid WHERE i.indrelid = 1 ORDER BY tabrelname, c.relname"
        );
    }

    #[test]
    fn test_any_to_array_contains() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RewriteArrayAnyAllOperation)];

        assert_rewrite!(
            &rules,
            "SELECT a = ANY(current_schemas(true))",
            "SELECT array_contains(current_schemas(true), a)"
        );

        assert_rewrite!(
            &rules,
            "SELECT a <> ALL(current_schemas(true))",
            "SELECT NOT array_contains(current_schemas(true), a)"
        );

        assert_rewrite!(
            &rules,
            "SELECT a = ANY('{r, l, e}')",
            "SELECT array_contains(ARRAY['r', 'l', 'e'], a)"
        );

        assert_rewrite!(
            &rules,
            "SELECT a FROM tbl WHERE a = ANY(current_schemas(true))",
            "SELECT a FROM tbl WHERE array_contains(current_schemas(true), a)"
        );
    }

    #[test]
    fn test_prepend_unqualified_table_name() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(PrependUnqualifiedPgTableName)];

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_catalog.pg_namespace",
            "SELECT * FROM pg_catalog.pg_namespace"
        );

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_namespace",
            "SELECT * FROM pg_catalog.pg_namespace"
        );

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_type",
            "SELECT * FROM pg_catalog.pg_type"
        );

        assert_rewrite!(
            &rules,
            "SELECT typtype, typname, pg_type.oid FROM pg_catalog.pg_type LEFT JOIN pg_namespace as ns ON ns.oid = oid",
            "SELECT typtype, typname, pg_type.oid FROM pg_catalog.pg_type LEFT JOIN pg_catalog.pg_namespace AS ns ON ns.oid = oid"
        );

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_compat_test",
            "SELECT * FROM pg_compat_test"
        );

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_custom_table",
            "SELECT * FROM pg_custom_table"
        );
    }

    #[test]
    fn test_array_literal_fix() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![Arc::new(FixArrayLiteral)];

        assert_rewrite!(
            &rules,
            "SELECT '{a, abc}'::text[]",
            "SELECT ARRAY['a', 'abc']::TEXT[]"
        );

        assert_rewrite!(
            &rules,
            "SELECT '{1, 2}'::int[]",
            "SELECT ARRAY[1, 2]::INT[]"
        );

        assert_rewrite!(
            &rules,
            "SELECT '{t, f}'::bool[]",
            "SELECT ARRAY[t, f]::BOOL[]"
        );
    }

    #[test]
    fn test_strip_callable_qualifier_from_table_function() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![Arc::new(StripCallableQualifier)];

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_catalog.pg_get_keywords()",
            "SELECT * FROM pg_get_keywords()"
        );
    }

    #[test]
    fn test_current_user() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(CurrentUserVariableToSessionUserFunctionCall)];

        // current_user -> session_user. `session_user` is a sqlparser-special
        // keyword, so it must stay parenthesis-less to round-trip through the
        // postgres dialect (re-parsed by DataFusion downstream).
        assert_rewrite!(&rules, "SELECT current_user", "SELECT session_user");
        assert_rewrite!(&rules, "SELECT CURRENT_USER", "SELECT session_user");
        assert_rewrite!(
            &rules,
            "SELECT is_null(current_user)",
            "SELECT is_null(session_user)"
        );

        // current_catalog -> current_database. `current_database` is NOT a
        // special keyword, so it needs explicit `()` to re-parse as a call.
        assert_rewrite!(
            &rules,
            "SELECT current_catalog",
            "SELECT current_database()"
        );

        // version is deliberately not rewritten: in postgres it is a regular
        // function, not a special register, so `SELECT version` (no parens) is
        // invalid and `SELECT version()` is already the canonical form.
        assert_rewrite!(&rules, "SELECT version", "SELECT version");
        assert_rewrite!(&rules, "SELECT version()", "SELECT version()");

        // current_schema -> current_schema()
        assert_rewrite!(&rules, "SELECT current_schema", "SELECT current_schema()");
    }

    #[test]
    fn test_strip_collate() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![Arc::new(StripCollate)];

        assert_rewrite!(
            &rules,
            "SELECT c.oid, c.relname FROM pg_catalog.pg_class c WHERE c.relname COLLATE pg_catalog.default = 'foo'",
            "SELECT c.oid, c.relname FROM pg_catalog.pg_class c WHERE c.relname = 'foo'"
        );

        // Combined with OPERATOR(...) syntax: only COLLATE is stripped here;
        // the operator is left for RewritePgCatalogOperator (next test).
        assert_rewrite!(
            &rules,
            "SELECT c.oid, c.relname FROM pg_catalog.pg_class c WHERE c.relname OPERATOR(pg_catalog.~) '^(tablename)$' COLLATE pg_catalog.default ORDER BY 2, 3;",
            "SELECT c.oid, c.relname FROM pg_catalog.pg_class c WHERE c.relname OPERATOR(pg_catalog.~) '^(tablename)$' ORDER BY 2, 3"
        );
    }

    #[test]
    fn test_rewrite_pg_catalog_operator() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![Arc::new(RewritePgCatalogOperator)];

        // All four pg_catalog regex operators map to their builtin forms.
        assert_rewrite!(
            &rules,
            "SELECT 'a' OPERATOR(pg_catalog.~) 'b'",
            "SELECT 'a' ~ 'b'"
        );
        assert_rewrite!(
            &rules,
            "SELECT 'a' OPERATOR(pg_catalog.~*) 'b'",
            "SELECT 'a' ~* 'b'"
        );
        assert_rewrite!(
            &rules,
            "SELECT 'a' OPERATOR(pg_catalog.!~) 'b'",
            "SELECT 'a' !~ 'b'"
        );
        assert_rewrite!(
            &rules,
            "SELECT 'a' OPERATOR(pg_catalog.!~*) 'b'",
            "SELECT 'a' !~* 'b'"
        );

        // The original psql \d shape: OPERATOR + COLLATE together. The operator
        // arm fires; COLLATE is left for StripCollate.
        assert_rewrite!(
            &rules,
            "SELECT c.oid, c.relname FROM pg_catalog.pg_class c WHERE c.relname OPERATOR(pg_catalog.~) '^(tablename)$' COLLATE pg_catalog.default ORDER BY 2, 3;",
            "SELECT c.oid, c.relname FROM pg_catalog.pg_class c WHERE c.relname ~ '^(tablename)$' COLLATE pg_catalog.default ORDER BY 2, 3"
        );
    }

    #[test]
    fn test_version_rewrite() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![Arc::new(FixVersionColumnName)];

        assert_rewrite!(&rules, "SELECT version()", "SELECT version() AS version");

        // Make sure we don't rewrite things we should leave alone
        assert_rewrite!(&rules, "SELECT version() as foo", "SELECT version() AS foo");
        assert_rewrite!(&rules, "SELECT version(foo)", "SELECT version(foo)");
        assert_rewrite!(&rules, "SELECT foo.version()", "SELECT foo.version()");
    }

    #[test]
    fn test_rewrite_regcast_numeric_string_to_literal() {
        // A numeric string resolves directly to the literal oid (Postgres
        // semantics), avoiding a subquery.
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RewriteRegCastToSubquery::new())];

        assert_rewrite!(&rules, "SELECT '16417'::regclass", "SELECT 16417");
        // pg_catalog.-qualified and uppercase spellings normalize to the same kind.
        assert_rewrite!(
            &rules,
            "SELECT '16417'::pg_catalog.regclass",
            "SELECT 16417"
        );
    }

    #[test]
    fn test_rewrite_regcast_name_string_to_subquery() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RewriteRegCastToSubquery::new())];

        // Name string -> scalar subquery against the backing catalog table.
        // The exact spelling of the subquery is part of the contract, so pin it.
        assert_rewrite!(
            &rules,
            "SELECT 'public'::regnamespace",
            "SELECT (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public')"
        );
        assert_rewrite!(
            &rules,
            "SELECT 'pg_class'::regclass",
            "SELECT (SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid CROSS JOIN (SELECT parse_ident('pg_class'::TEXT) AS parts) WHERE array_length(parts, 1) IN (1, 2) AND ((array_length(parts, 1) = 1 AND n.nspname = current_schema() AND c.relname = parts[1]) OR (array_length(parts, 1) = 2 AND n.nspname = parts[1] AND c.relname = parts[2])))"
        );
        assert_rewrite!(
            &rules,
            "SELECT 'array_in'::regproc",
            "SELECT (SELECT oid FROM pg_catalog.pg_proc WHERE proname = 'array_in')"
        );
        assert_rewrite!(
            &rules,
            "SELECT 'integer'::regtype",
            "SELECT (SELECT oid FROM pg_catalog.pg_type WHERE typname = 'integer')"
        );

        // Works inside a comparison, where clients actually use it.
        assert_rewrite!(
            &rules,
            "SELECT 1 WHERE classoid = 'pg_class'::regclass",
            "SELECT 1 WHERE classoid = (SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid CROSS JOIN (SELECT parse_ident('pg_class'::TEXT) AS parts) WHERE array_length(parts, 1) IN (1, 2) AND ((array_length(parts, 1) = 1 AND n.nspname = current_schema() AND c.relname = parts[1]) OR (array_length(parts, 1) = 2 AND n.nspname = parts[1] AND c.relname = parts[2])))"
        );
    }

    #[test]
    fn test_rewrite_regclass_uses_parse_ident_schema_aware_lookup() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RewriteRegCastToSubquery::new())];

        let lookup = "SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid CROSS JOIN (SELECT parse_ident($1::TEXT) AS parts) WHERE array_length(parts, 1) IN (1, 2) AND ((array_length(parts, 1) = 1 AND n.nspname = current_schema() AND c.relname = parts[1]) OR (array_length(parts, 1) = 2 AND n.nspname = parts[1] AND c.relname = parts[2]))";

        // A bound portal parameter remains a parameter through the parse_ident
        // lookup and outer oid cast.
        assert_rewrite!(
            &rules,
            "SELECT $1::regclass::oid",
            format!("SELECT ({lookup})::oid")
        );
        // The pg_catalog-qualified spelling normalizes to the same template.
        assert_rewrite!(
            &rules,
            "SELECT $1::pg_catalog.regclass",
            format!("SELECT ({lookup})")
        );
        // parse_ident preserves quoted, case-sensitive unqualified names.
        assert_rewrite!(
            &rules,
            "SELECT '\"Mixed Case\"'::regclass",
            format!("SELECT ({})", lookup.replace("$1", "'\"Mixed Case\"'"))
        );
        // Two explicit quoted components select the supplied schema and relation.
        assert_rewrite!(
            &rules,
            "SELECT '\"Mixed Schema\".\"Mixed Table\"'::regclass",
            format!(
                "SELECT ({})",
                lookup.replace("$1", "'\"Mixed Schema\".\"Mixed Table\"'")
            )
        );
        // Invalid and over-qualified names are guarded by the component count.
        assert_rewrite!(
            &rules,
            "SELECT 'too.many.parts'::regclass",
            format!("SELECT ({})", lookup.replace("$1", "'too.many.parts'"))
        );
        assert_rewrite!(
            &rules,
            "SELECT 'too..many'::regclass",
            format!("SELECT ({})", lookup.replace("$1", "'too..many'"))
        );
    }

    #[test]
    fn test_rewrite_regclass_leaves_direct_relname_predicate_unchanged() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RewriteRegCastToSubquery::new())];

        assert_rewrite!(
            &rules,
            "SELECT oid FROM pg_catalog.pg_class WHERE relname = $1",
            "SELECT oid FROM pg_catalog.pg_class WHERE relname = $1"
        );
    }

    #[test]
    fn test_rewrite_regcast_leaves_reverse_and_unknown_alone() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RewriteRegCastToSubquery::new())];

        // Reverse direction (column operand) is NOT rewritten -- left for
        // PgOidTypePlanner so the cast still parses.
        assert_rewrite!(
            &rules,
            "SELECT prorettype::regtype",
            "SELECT prorettype::regtype"
        );
        // A cast whose type is not one of the four resolvable oid-alias kinds
        // is left alone (not crashed on, not rewritten).
        assert_rewrite!(&rules, "SELECT 'x'::regrole", "SELECT 'x'::regrole");
        // A numeric operand cast to a non-oid type is left alone.
        assert_rewrite!(&rules, "SELECT '1'::int4", "SELECT '1'::INT4");
    }
}
