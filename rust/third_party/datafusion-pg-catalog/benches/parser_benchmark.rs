use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_pg_catalog::sql::PostgresCompatibilityParser;

fn bench_parser_parse(c: &mut Criterion) {
    let parser = PostgresCompatibilityParser::new();

    // Simple queries
    let simple_queries = vec![
        "SELECT * FROM users",
        "SELECT id, name FROM users WHERE age > 18",
        "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
        "UPDATE users SET name = 'Jane' WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
    ];

    c.bench_function("parse_simple_queries", |b| {
        b.iter(|| {
            for query in &simple_queries {
                black_box(parser.parse(black_box(query)).unwrap());
            }
        })
    });

    // Complex queries from the blacklist
    let complex_queries = vec![
        // pgcli startup query
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

        // psql \d <table> query
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
    ];

    c.bench_function("parse_complex_queries", |b| {
        b.iter(|| {
            for query in &complex_queries {
                if let Ok(result) = parser.parse(black_box(query)) {
                    black_box(result);
                }
            }
        })
    });
}

fn bench_parser_creation(c: &mut Criterion) {
    c.bench_function("parser_creation", |b| {
        b.iter(|| {
            black_box(PostgresCompatibilityParser::new());
        })
    });
}

criterion_group!(benches, bench_parser_parse, bench_parser_creation);
criterion_main!(benches);
