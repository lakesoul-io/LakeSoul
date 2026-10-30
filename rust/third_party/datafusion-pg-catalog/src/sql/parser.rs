use std::sync::Arc;

use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::parser::ParserError;
use datafusion::sql::sqlparser::tokenizer::Token;
use datafusion::sql::sqlparser::tokenizer::TokenWithSpan;

use super::rules::AliasDuplicatedProjectionRewrite;
use super::rules::CurrentUserVariableToSessionUserFunctionCall;
use super::rules::FixArrayLiteral;
use super::rules::FixVersionColumnName;
use super::rules::PrependUnqualifiedPgTableName;
use super::rules::ResolveUnqualifiedIdentifier;
use super::rules::RewriteArrayAnyAllOperation;
use super::rules::RewritePgCatalogOperator;
use super::rules::RewriteRegCastToSubquery;
use super::rules::SqlStatementRewriteRule;
use super::rules::StripCallableQualifier;
use super::rules::StripCollate;

/// Last-resort replacements for whole client queries that DataFusion cannot
/// execute and cannot be fixed by a SQL rewrite rule.
///
/// Each entry is a `(client_sql, replacement_sql)` pair. The parser tokenizes
/// both at construction time (see [`PostgresCompatibilityParser::new`]) and the
/// input token stream is scanned for the client pattern; on a match the matched
/// tokens are replaced wholesale, before any rewrite rule runs.
///
/// # How matching works
///
/// * **Token-level, whitespace-insensitive:** only non-whitespace, non-`;`
///   tokens are compared, so formatting differences (indentation, newlines,
///   spacing) do not matter.
/// * **Substring match:** patterns are scanned at *every* token position, so a
///   pattern matches as a contiguous sub-sequence of the input. Most entries
///   are whole statements and therefore only match at a statement boundary;
///   the grafana entry below is intentionally a *partial* replacement (it
///   substitutes just an inner subquery).
/// * **`$N` placeholders are wildcards:** a `Token::Placeholder` in the pattern
///   (e.g. `$1`) matches any single non-`;` token in the input, so the psql
///   `\d` entries match regardless of whether the client binds a parameter or
///   sends a literal oid string.
///
/// # When to add or remove an entry
///
/// These entries exist solely because of a specific DataFusion limitation.
/// Each entry's comment names the limitation and a removal condition. Before
/// adding a new entry, check whether a general SQL rewrite rule (or better, an
/// analyzer rule / type planner, as was done for oid resolution) can handle it
/// instead -- those are preferred because they are metadata-aware and
/// order-independent.
const BLACKLIST_SQL_MAPPING: &[(&str, &str)] = &[
    // pgcli -- foreign-key column lookup
    //
    // Blocked by: `unnest(<scalar subquery>)` -- DataFusion cannot unnest the
    //   result of a subquery (here, `array_agg(...)` over a correlated join).
    // Remove when: DF supports unnesting arbitrary subquery results.
    (
"SELECT s_p.nspname AS parentschema,
                               t_p.relname AS parenttable,
                               unnest((
                                select
                                    array_agg(attname ORDER BY i)
                                from
                                    (select unnest(confkey) as attnum, generate_subscripts(confkey, 1) as i) x
                                    JOIN pg_catalog.pg_attribute c USING(attnum)
                                    WHERE c.attrelid = fk.confrelid
                                )) AS parentcolumn,
                               s_c.nspname AS childschema,
                               t_c.relname AS childtable,
                               unnest((
                                select
                                    array_agg(attname ORDER BY i)
                                from
                                    (select unnest(conkey) as attnum, generate_subscripts(conkey, 1) as i) x
                                    JOIN pg_catalog.pg_attribute c USING(attnum)
                                    WHERE c.attrelid = fk.conrelid
                                )) AS childcolumn
                        FROM pg_catalog.pg_constraint fk
                        JOIN pg_catalog.pg_class      t_p ON t_p.oid = fk.confrelid
                        JOIN pg_catalog.pg_namespace  s_p ON s_p.oid = t_p.relnamespace
                        JOIN pg_catalog.pg_class      t_c ON t_c.oid = fk.conrelid
                        JOIN pg_catalog.pg_namespace  s_c ON s_c.oid = t_c.relnamespace
                        WHERE fk.contype = 'f'",
"SELECT
   NULL::TEXT AS parentschema,
   NULL::TEXT AS parenttable,
   NULL::TEXT AS parentcolumn,
   NULL::TEXT AS childschema,
   NULL::TEXT AS childtable,
   NULL::TEXT AS childcolumn
 WHERE false"),

    // pgcli -- user-defined type lookup
    //
    // Blocked by: correlated scalar subquery in the WHERE clause --
    //   `SELECT c.relkind = 'c' FROM pg_catalog.pg_class c
    //    WHERE c.oid = t.typrelid` references the outer `t`.
    // Remove when: DF plans correlated scalar subqueries in predicates.
    (
"SELECT n.nspname schema_name,
                                       t.typname type_name
                                FROM   pg_catalog.pg_type t
                                       INNER JOIN pg_catalog.pg_namespace n
                                          ON n.oid = t.typnamespace
                                WHERE ( t.typrelid = 0  -- non-composite types
                                        OR (  -- composite type, but not a table
                                              SELECT c.relkind = 'c'
                                              FROM pg_catalog.pg_class c
                                              WHERE c.oid = t.typrelid
                                            )
                                      )
                                      AND NOT EXISTS( -- ignore array types
                                            SELECT  1
                                            FROM    pg_catalog.pg_type el
                                            WHERE   el.oid = t.typelem AND el.typarray = t.oid
                                          )
                                      AND n.nspname <> 'pg_catalog'
                                      AND n.nspname <> 'information_schema'
                                ORDER BY 1, 2;",
"SELECT NULL::TEXT AS schema_name, NULL::TEXT AS type_name WHERE false"
    ),

    // psql \d <table> -- row policies (polrelid bound via the $1 wildcard)
    //
    // Blocked by: the `array(SELECT ...)` array constructor (here over a
    //   join on `any(pol.polroles)`). DataFusion lacks the array-from-subquery
    //   constructor syntax.
    // Remove when: DF supports the `array(SELECT ...)` constructor.
    (
"SELECT pol.polname, pol.polpermissive,
          CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END,
          pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),
          pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),
          CASE pol.polcmd
            WHEN 'r' THEN 'SELECT'
            WHEN 'a' THEN 'INSERT'
            WHEN 'w' THEN 'UPDATE'
            WHEN 'd' THEN 'DELETE'
            END AS cmd
        FROM pg_catalog.pg_policy pol
        WHERE pol.polrelid = $1 ORDER BY 1;",
"SELECT
   NULL::TEXT AS polname,
   NULL::TEXT AS polpermissive,
   NULL::TEXT AS array_to_string,
   NULL::TEXT AS pg_get_expr_1,
   NULL::TEXT AS pg_get_expr_2,
   NULL::TEXT AS cmd
 WHERE false"
    ),

    // psql \d <table> -- extended statistics (stxrelid bound via $1)
    //
    // Blocked by: `<char-literal> = any(<array-column>)` -- `stxkind` is a
    //   `char[2]`; DataFusion's `array_has` expects matching element types and
    //   rejects the char-vs-string comparison, so all three `any(stxkind)`
    //   predicates fail to plan.
    // Remove when: DF coerces element types in `any(array_col)` comparisons,
    //   or an analyzer rule lowers `lit = ANY(array_col)` to a proper element
    //   comparison.
    (
"SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS nsp, stxname,
        pg_catalog.pg_get_statisticsobjdef_columns(oid) AS columns,
          'd' = any(stxkind) AS ndist_enabled,
          'f' = any(stxkind) AS deps_enabled,
          'm' = any(stxkind) AS mcv_enabled,
        stxstattarget
        FROM pg_catalog.pg_statistic_ext
        WHERE stxrelid = $1
        ORDER BY nsp, stxname;",
"SELECT
   NULL::INT AS oid,
   NULL::TEXT AS stxrelid,
   NULL::TEXT AS nsp,
   NULL::TEXT AS stxname,
   NULL::TEXT AS columns,
   NULL::BOOLEAN AS ndist_enabled,
   NULL::BOOLEAN AS deps_enabled,
   NULL::BOOLEAN AS mcv_enabled,
   NULL::TEXT AS stxstattarget
 WHERE false"
    ),

    // psql \d <table> -- publications (prrelid / oid bound via $1)
    //
    // Blocked by: the three UNION branches each project two unnamed `NULL`
    //   columns, yielding duplicate/empty projection names that DataFusion
    //   rejects. (The `generate_series(0, array_upper(...))` over a correlated
    //   `int2[]` would also fail, but the duplicate-name error fires first.)
    // Remove when: DF tolerates duplicate/unnamed NULL projection columns,
    //   or an aliasing rule can assign synthetic names.
    (
"SELECT pubname
             , NULL
             , NULL
        FROM pg_catalog.pg_publication p
             JOIN pg_catalog.pg_publication_namespace pn ON p.oid = pn.pnpubid
             JOIN pg_catalog.pg_class pc ON pc.relnamespace = pn.pnnspid
        WHERE pc.oid = $1 and pg_catalog.pg_relation_is_publishable($1)
        UNION
        SELECT pubname
             , pg_get_expr(pr.prqual, c.oid)
             , (CASE WHEN pr.prattrs IS NOT NULL THEN
                 (SELECT string_agg(attname, ', ')
                   FROM pg_catalog.generate_series(0, pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s,
                        pg_catalog.pg_attribute
                  WHERE attrelid = pr.prrelid AND attnum = prattrs[s])
                ELSE NULL END) FROM pg_catalog.pg_publication p
             JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid
             JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid
        WHERE pr.prrelid = $1
        UNION
        SELECT pubname
             , NULL
             , NULL
        FROM pg_catalog.pg_publication p
        WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable($1)
        ORDER BY 1;",
"SELECT
   NULL::TEXT AS pubname,
   NULL::TEXT AS _1,
   NULL::TEXT AS _2
 WHERE false"
    ),

    // dbeaver -- relation size lookup. NOTE: re-blacklisted because resolving
    // the implicit `c.relnamespace = 'public'` comparison (bare string vs an
    // oid column) requires schema awareness, which the SQL-rewrite layer does
    // not have. The explicit `'...'::regclass` casts elsewhere are handled by
    // RewriteRegCastToSubquery; only this implicit form needs the stub. The rest
    // of the query (pg_total_relation_size / pg_relation_size) works.
    //
    // Blocked by: implicit `<oid-col> = '<name>'` needs the column's oid-alias
    //   metadata, only available at the analyzer layer.
    // Remove when: an analyzer rule resolves implicit string-vs-oid-column
    //   comparisons (the former OidStringCoercion rule did this), or the client
    //   switches to the explicit `'...'::regnamespace` cast form.
    (
"select c.oid,pg_catalog.pg_total_relation_size(c.oid) as total_rel_size,pg_catalog.pg_relation_size(c.oid) as rel_size\n     FROM pg_class c\n     WHERE c.relnamespace='public'",
"SELECT\n   NULL::INT AS oid,\n   NULL::INT AS total_rel_size,\n   NULL::INT AS rel_size\n WHERE false"
    ),

    // grafana -- search_path membership lookup. NOTE: this is the only entry
    // that is a *partial* replacement -- it matches just the inner subquery and
    // substitutes the empty string `''`, so the surrounding `IN (...)` simply
    // matches nothing and the query returns zero rows.
    //
    // Blocked by: missing `current_setting()` UDF -- the query resolves the
    //   session `search_path` at runtime via `current_setting('search_path')`.
    //   (generate_series over dynamic array bounds now works via the
    //   CoerceIntArgsToBigInt wrapper; current_setting is the remaining gap.)
    // Remove when: a `current_setting()` UDF is registered.
    (r#"SELECT
            CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END
        FROM
            generate_series(
                array_lower(string_to_array(current_setting('search_path'),','),1),
                array_upper(string_to_array(current_setting('search_path'),','),1)
            ) as i,
            string_to_array(current_setting('search_path'),',') s"#,
"''")
];

/// A parser with Postgres Compatibility for Datafusion
///
/// This parser will try its best to rewrite postgres SQL into a form that
/// DataFusion supports. It also maintains a blacklist that will transform the
/// statement to a similar version if rewriting it is not worth the effort for
/// now.
#[derive(Debug)]
pub struct PostgresCompatibilityParser {
    blacklist: Vec<(Vec<Token>, Vec<Token>)>,
    rewrite_rules: Vec<Arc<dyn SqlStatementRewriteRule>>,
}

impl Default for PostgresCompatibilityParser {
    fn default() -> Self {
        Self::new()
    }
}

impl PostgresCompatibilityParser {
    pub fn new() -> Self {
        let mut mapping = Vec::with_capacity(BLACKLIST_SQL_MAPPING.len());

        for (sql_from, sql_to) in BLACKLIST_SQL_MAPPING {
            mapping.push((
                Parser::new(&PostgreSqlDialect {})
                    .try_with_sql(sql_from)
                    .unwrap()
                    .into_tokens()
                    .into_iter()
                    .map(|t| t.token)
                    .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
                    .collect(),
                Parser::new(&PostgreSqlDialect {})
                    .try_with_sql(sql_to)
                    .unwrap()
                    .into_tokens()
                    .into_iter()
                    .map(|t| t.token)
                    .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
                    .collect(),
            ));
        }

        let rewrite_rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![
            // The blacklist substitution in `parse()` runs before any of
            // these rules, so by the time they see the statement any
            // blacklisted fragment has already been replaced.
            Arc::new(AliasDuplicatedProjectionRewrite),
            Arc::new(ResolveUnqualifiedIdentifier),
            Arc::new(RewriteArrayAnyAllOperation),
            Arc::new(PrependUnqualifiedPgTableName),
            Arc::new(StripCallableQualifier),
            Arc::new(FixArrayLiteral),
            Arc::new(CurrentUserVariableToSessionUserFunctionCall),
            Arc::new(StripCollate),
            Arc::new(RewritePgCatalogOperator),
            Arc::new(RewriteRegCastToSubquery::new()),
            // Leave projection scalar subqueries to DataFusion's planner,
            // including correlation and parameters. ScalarSubqueryExec checks
            // uncorrelated cardinality; rewriting to LIMIT 1/NULL loses it.
            Arc::new(FixVersionColumnName),
        ];

        Self {
            blacklist: mapping,
            rewrite_rules,
        }
    }

    /// return tokens with replacements applied
    fn maybe_replace_tokens(&self, input: &str) -> Result<Vec<Token>, ParserError> {
        let parser = Parser::new(&PostgreSqlDialect {});
        let tokens = parser.try_with_sql(input)?.into_tokens();

        // Drop whitespace tokens; keep semicolons as they separate statements.
        let filtered_tokens: Vec<Token> = tokens
            .iter()
            .map(|t| t.token.clone())
            .filter(|t| !matches!(t, Token::Whitespace(_)))
            .collect();

        // Rewrite the Postgres `ABORT` statement to `ROLLBACK` (they are
        // synonyms). This must happen at the token level because sqlparser does
        // not parse `ABORT` as a statement, so it cannot be a post-parse
        // [`SqlStatementRewriteRule`].
        //
        // TODO: remove when https://github.com/apache/datafusion-sqlparser-rs/pull/2332
        //   is released in the version of sqlparser we depend on.
        let filtered_tokens = Self::rewrite_abort_to_rollback(filtered_tokens);

        // Handle empty input
        if filtered_tokens.is_empty() {
            return Ok(Vec::new());
        }

        // Build result by processing filtered tokens sequentially
        let mut result = Vec::new();
        let mut i = 0;

        while i < filtered_tokens.len() {
            // Keep semicolons as-is
            if matches!(&filtered_tokens[i], Token::SemiColon) {
                result.push(filtered_tokens[i].clone());
                i += 1;
                continue;
            }

            // Try to find a blacklist pattern match starting at this position
            let mut matched = false;
            for (pattern, replacement) in &self.blacklist {
                if pattern.is_empty() {
                    continue;
                }

                // Check if we have enough tokens remaining
                let mut j = 0;
                let mut pattern_idx = 0;
                while i + j < filtered_tokens.len() && pattern_idx < pattern.len() {
                    // Skip semicolons in the input when matching patterns
                    if matches!(&filtered_tokens[i + j], Token::SemiColon) {
                        j += 1;
                        continue;
                    }

                    match &pattern[pattern_idx] {
                        Token::Placeholder(_) => {
                            // Placeholder matches any non-semicolon token
                            pattern_idx += 1;
                            j += 1;
                        }
                        _ => {
                            if filtered_tokens[i + j] != pattern[pattern_idx] {
                                break;
                            }
                            pattern_idx += 1;
                            j += 1;
                        }
                    }
                }

                // Check if we matched the entire pattern
                if pattern_idx == pattern.len() {
                    // Add replacement tokens
                    result.extend(replacement.iter().cloned());
                    // Skip the matched pattern (including any semicolons we skipped)
                    i += j;
                    matched = true;
                    break;
                }
            }

            if !matched {
                // No match, keep the original token
                result.push(filtered_tokens[i].clone());
                i += 1;
            }
        }

        Ok(result)
    }

    /// Replace every `ABORT` keyword token with `ROLLBACK` (see
    /// [`Self::maybe_replace_tokens`] for rationale).
    fn rewrite_abort_to_rollback(tokens: Vec<Token>) -> Vec<Token> {
        tokens
            .into_iter()
            .map(|t| {
                if matches!(&t, Token::Word(w) if w.keyword == Keyword::ABORT) {
                    Token::make_keyword("ROLLBACK")
                } else {
                    t
                }
            })
            .collect()
    }

    fn parse_tokens(&self, tokens: Vec<Token>) -> Result<Vec<Statement>, ParserError> {
        let parser = Parser::new(&PostgreSqlDialect {});
        // Convert tokens to TokenWithSpan with dummy spans
        let tokens_with_spans: Vec<TokenWithSpan> = tokens
            .into_iter()
            .map(|token| TokenWithSpan {
                token,
                span: datafusion::sql::sqlparser::tokenizer::Span::empty(),
            })
            .collect();
        parser
            .with_tokens_with_locations(tokens_with_spans)
            .parse_statements()
    }

    pub fn parse(&self, input: &str) -> Result<Vec<Statement>, ParserError> {
        let tokens = self.maybe_replace_tokens(input)?;
        let statements = self.parse_tokens(tokens)?;

        let statements: Vec<_> = statements.into_iter().map(|s| self.rewrite(s)).collect();
        Ok(statements)
    }

    pub fn rewrite(&self, mut s: Statement) -> Statement {
        for rule in &self.rewrite_rules {
            s = rule.rewrite(s);
        }

        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_match() {
        let sql = "SELECT pol.polname, pol.polpermissive,
              CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END,
              pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),
              pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),
              CASE pol.polcmd
                WHEN 'r' THEN 'SELECT'
                WHEN 'a' THEN 'INSERT'
                WHEN 'w' THEN 'UPDATE'
                WHEN 'd' THEN 'DELETE'
                END AS cmd
            FROM pg_catalog.pg_policy pol
            WHERE pol.polrelid = '16384' ORDER BY 1;";

        let parser = PostgresCompatibilityParser::new();
        let actual_tokens = parser
            .maybe_replace_tokens(sql)
            .expect("failed to parse sql")
            .into_iter()
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
            .collect::<Vec<_>>();

        let expected_sql = r#"SELECT
   NULL::TEXT AS polname,
   NULL::TEXT AS polpermissive,
   NULL::TEXT AS array_to_string,
   NULL::TEXT AS pg_get_expr_1,
   NULL::TEXT AS pg_get_expr_2,
   NULL::TEXT AS cmd
 WHERE false"#;

        let expected_tokens = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(expected_sql)
            .unwrap()
            .into_tokens()
            .into_iter()
            .map(|t| t.token)
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
            .collect::<Vec<_>>();

        assert_eq!(actual_tokens, expected_tokens);

        let sql = "SELECT n.nspname schema_name,
                                       t.typname type_name
                                FROM   pg_catalog.pg_type t
                                       INNER JOIN pg_catalog.pg_namespace n
                                          ON n.oid = t.typnamespace
                                WHERE ( t.typrelid = 0  -- non-composite types
                                        OR (  -- composite type, but not a table
                                              SELECT c.relkind = 'c'
                                              FROM pg_catalog.pg_class c
                                              WHERE c.oid = t.typrelid
                                            )
                                      )
                                      AND NOT EXISTS( -- ignore array types
                                            SELECT  1
                                            FROM    pg_catalog.pg_type el
                                            WHERE   el.oid = t.typelem AND el.typarray = t.oid
                                          )
                                      AND n.nspname <> 'pg_catalog'
                                      AND n.nspname <> 'information_schema'
                                ORDER BY 1, 2";

        let parser = PostgresCompatibilityParser::new();

        let actual_tokens = parser
            .maybe_replace_tokens(sql)
            .expect("failed to parse sql")
            .into_iter()
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
            .collect::<Vec<_>>();

        let expected_sql =
            r#"SELECT NULL::TEXT AS schema_name, NULL::TEXT AS type_name WHERE false"#;

        let expected_tokens = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(expected_sql)
            .unwrap()
            .into_tokens()
            .into_iter()
            .map(|t| t.token)
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
            .collect::<Vec<_>>();

        assert_eq!(actual_tokens, expected_tokens);

        let sql = "SELECT pubname
             , NULL
             , NULL
        FROM pg_catalog.pg_publication p
             JOIN pg_catalog.pg_publication_namespace pn ON p.oid = pn.pnpubid
             JOIN pg_catalog.pg_class pc ON pc.relnamespace = pn.pnnspid
        WHERE pc.oid ='16384' and pg_catalog.pg_relation_is_publishable('16384')
        UNION
        SELECT pubname
             , pg_get_expr(pr.prqual, c.oid)
             , (CASE WHEN pr.prattrs IS NOT NULL THEN
                 (SELECT string_agg(attname, ', ')
                   FROM pg_catalog.generate_series(0, pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s,
                        pg_catalog.pg_attribute
                  WHERE attrelid = pr.prrelid AND attnum = prattrs[s])
                ELSE NULL END) FROM pg_catalog.pg_publication p
             JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid
             JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid
        WHERE pr.prrelid = '16384'
        UNION
        SELECT pubname
             , NULL
             , NULL
        FROM pg_catalog.pg_publication p
        WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('16384')
        ORDER BY 1;";

        let parser = PostgresCompatibilityParser::new();

        let actual_tokens = parser
            .maybe_replace_tokens(sql)
            .expect("failed to parse sql")
            .into_iter()
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
            .collect::<Vec<_>>();

        let expected_sql = r#"SELECT
   NULL::TEXT AS pubname,
   NULL::TEXT AS _1,
   NULL::TEXT AS _2
 WHERE false"#;

        let expected_tokens = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(expected_sql)
            .unwrap()
            .into_tokens()
            .into_iter()
            .map(|t| t.token)
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
            .collect::<Vec<_>>();

        assert_eq!(actual_tokens, expected_tokens);
    }

    #[test]
    fn test_empty_query() {
        let parser = PostgresCompatibilityParser::new();
        let result = parser.parse(" ").expect("failed to parse sql");
        assert!(result.is_empty());

        let result = parser.parse("").expect("failed to parse sql");
        assert!(result.is_empty());

        let result = parser.parse(";").expect("failed to parse sql");
        assert!(result.is_empty());
    }

    #[test]
    fn test_partial_match() {
        let parser = PostgresCompatibilityParser::new();

        // Test partial match where the beginning matches a blacklisted query
        // Using a simpler query that doesn't have placeholders for easier testing
        let sql = r#"SELECT
        CASE WHEN
              quote_ident(table_schema) IN (
              SELECT
                CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END
              FROM
                generate_series(
                  array_lower(string_to_array(current_setting('search_path'),','),1),
                  array_upper(string_to_array(current_setting('search_path'),','),1)
                ) as i,
                string_to_array(current_setting('search_path'),',') s
              )
          THEN quote_ident(table_name)
          ELSE quote_ident(table_schema) || '.' || quote_ident(table_name)
        END AS "table"
        FROM information_schema.tables
        WHERE quote_ident(table_schema) NOT IN ('information_schema',
                                 'pg_catalog',
                                 '_timescaledb_cache',
                                 '_timescaledb_catalog',
                                 '_timescaledb_internal',
                                 '_timescaledb_config',
                                 'timescaledb_information',
                                 'timescaledb_experimental')
        ORDER BY CASE WHEN
              quote_ident(table_schema) IN (
              SELECT
                CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END
              FROM
                generate_series(
                  array_lower(string_to_array(current_setting('search_path'),','),1),
                  array_upper(string_to_array(current_setting('search_path'),','),1)
                ) as i,
                string_to_array(current_setting('search_path'),',') s
              ) THEN 0 ELSE 1 END, 1"#;

        let tokens = parser
            .maybe_replace_tokens(sql)
            .expect("failed to parse sql");
        // Should have the beginning replaced with 'SELECT' and the rest preserved
        assert!(tokens.len() > 0);

        let expected_sql = r#"SELECT
        CASE WHEN
              quote_ident(table_schema) IN (
              '')
          THEN quote_ident(table_name)
          ELSE quote_ident(table_schema) || '.' || quote_ident(table_name)
        END AS "table"
        FROM information_schema.tables
        WHERE quote_ident(table_schema) NOT IN ('information_schema',
                                 'pg_catalog',
                                 '_timescaledb_cache',
                                 '_timescaledb_catalog',
                                 '_timescaledb_internal',
                                 '_timescaledb_config',
                                 'timescaledb_information',
                                 'timescaledb_experimental')
        ORDER BY CASE WHEN
              quote_ident(table_schema) IN (
              ''
              ) THEN 0 ELSE 1 END, 1"#;

        let expected_tokens = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(expected_sql)
            .unwrap()
            .into_tokens();

        // Compare token values (ignoring spans and whitespace)
        let actual_tokens: Vec<_> = tokens
            .iter()
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
            .collect();
        let expected_token_values: Vec<_> = expected_tokens
            .iter()
            .map(|t| &t.token)
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
            .collect();

        assert_eq!(actual_tokens, expected_token_values);
    }

    #[test]
    fn test_abort_rewritten_to_rollback() {
        // ABORT is a Postgres synonym for ROLLBACK; sqlparser does not parse it
        // as a statement, so the token-level rewrite_abort_to_rollback turns
        // it into ROLLBACK before parsing.
        let parser = PostgresCompatibilityParser::new();
        let stmts = parser.parse("ABORT").expect("failed to parse");
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0].to_string(), "ROLLBACK");

        // `ABORT AND CHAIN` -> `ROLLBACK AND CHAIN`.
        let stmts = parser.parse("ABORT AND CHAIN").expect("failed to parse");
        assert_eq!(stmts[0].to_string(), "ROLLBACK AND CHAIN");

        // A column named `abort` (quoted) must NOT be rewritten -- only the
        // bare keyword form is a statement.
        let stmts = parser
            .parse("SELECT \"abort\" FROM t")
            .expect("failed to parse");
        assert!(stmts[0].to_string().contains("\"abort\""));
    }
}
