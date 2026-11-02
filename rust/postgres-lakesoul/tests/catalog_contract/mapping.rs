// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Phase-three namespace/database mapping contracts against the real server.
//!
//! LakeSoul maps one namespace to one PostgreSQL *database*: connecting with
//! `dbname=<namespace>` exposes that namespace's tables under the single user
//! schema `public`, never as a schema named after the namespace. Two real
//! client connections (primary and a peer namespace) share one child process,
//! so these cases can tell "same server, different database" apart from
//! "different server" and from "session/factory identity leaked between
//! databases".
//!
//! Every expected row is independently known from the harness fixtures; the
//! reference PostgreSQL connection is not consulted, because its database and
//! schema identity (and the search_path used to isolate it) are an artefact of
//! the harness, not the LakeSoul contract.

use anyhow::{Context as _, Result, ensure};
use tokio_postgres::Client;
use tokio_postgres::types::Type;

use crate::common;
use crate::support::Harness;
use crate::support::compare::Rows;

/// A consumer picks the database/schema it is attached to from the session
/// identity; a wrong answer silently reads another namespace.
const BUG_IDENTITY: &str = "consumer-visible bug: current_database()/current_schema() report the wrong \
     database, or the LakeSoul namespace leaks as a schema instead of the default public schema, \
     so clients resolve the wrong namespace's objects";

/// A consumer reading a table by bare and by `public.`-qualified name must get
/// exactly this database's rows, before and after a peer database was used.
const BUG_MAPPING_ROWS: &str = "consumer-visible bug: bare or public-qualified reads return another \
     database's rows (cross-database table identity leakage), or the namespace-to-public mapping is \
     missing";

/// Reusing a peer connection must not mutate the shared factory catalog view
/// or an existing/fresh primary connection's identity.
const BUG_FACTORY_LEAK: &str = "consumer-visible bug: using a connection to a peer database mutates \
     the shared factory catalog/session identity, so an existing or freshly opened primary \
     connection silently starts reading the peer namespace";

const BUG_PG_DATABASE: &str = "consumer-visible bug: a visible LakeSoul namespace is missing from \
     pg_database, listed more than once, or replaced by another database name, so clients cannot \
     discover which databases they may connect to";

const BUG_PG_NAMESPACE: &str = "consumer-visible bug: LakeSoul namespace names leak into pg_namespace \
     as schemas (or public/pg_catalog are missing), so drivers resolve user tables under the wrong \
     schema namespace";

const BUG_PG_CLASS: &str = "consumer-visible bug: pg_class over public omits or adds user relations for \
     the connected database, so clients cannot enumerate this database's tables (or see another's)";

const BUG_PG_ATTRIBUTE: &str = "consumer-visible bug: pg_attribute resolves public.mapping_items' \
     relation OID to the wrong relation/columns, breaking column discovery such as psql \\d and \
     hiding which database a same-named relation belongs to";

/// SQL single-quoted literal. Harness names are generated, but escaping here
/// keeps the cases deterministic if that ever changes.
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn primary_mapping_rows() -> Rows {
    common::rows(&[&[Some("101"), Some("alpha")], &[Some("102"), Some("alpha")]])
}

fn peer_mapping_rows() -> Rows {
    common::rows(&[&[Some("201"), Some("beta")], &[Some("202"), Some("beta")]])
}

fn primary_relations() -> Rows {
    common::rows(&[
        &[Some("empty_items"), Some("r")],
        &[Some("items"), Some("r")],
        &[Some("mapping_items"), Some("r")],
        &[Some("only_a"), Some("r")],
        &[Some("pk_items"), Some("r")],
        &[Some("range_items"), Some("r")],
    ])
}

fn peer_relations() -> Rows {
    common::rows(&[
        &[Some("mapping_items"), Some("r")],
        &[Some("only_b"), Some("r")],
    ])
}

/// Every public relation mapped to its ordered user columns. The layouts are
/// known from the fixture DDL, so a relation OID resolved against another
/// database (or a same-named relation from the wrong catalog) shows up here.
fn primary_columns() -> Rows {
    common::rows(&[
        &[Some("empty_items"), Some("id")],
        &[Some("empty_items"), Some("val")],
        &[Some("empty_items"), Some("label")],
        &[Some("items"), Some("id")],
        &[Some("items"), Some("val")],
        &[Some("items"), Some("label")],
        &[Some("mapping_items"), Some("id")],
        &[Some("mapping_items"), Some("origin")],
        &[Some("only_a"), Some("id")],
        &[Some("pk_items"), Some("id")],
        &[Some("pk_items"), Some("val")],
        &[Some("pk_items"), Some("label")],
        &[Some("range_items"), Some("part")],
        &[Some("range_items"), Some("id")],
        &[Some("range_items"), Some("val")],
    ])
}

fn peer_columns() -> Rows {
    common::rows(&[
        &[Some("mapping_items"), Some("id")],
        &[Some("mapping_items"), Some("origin")],
        &[Some("only_b"), Some("id")],
    ])
}

/// Identity of the selected database plus the default user schema.
async fn assert_identity(
    client: &Client,
    id: &str,
    bug: &str,
    database: &str,
) -> Result<()> {
    common::assert_query(
        client,
        id,
        bug,
        "SELECT current_database()::text AS cdb, current_schema()::text AS csch",
        &[("cdb", Type::TEXT), ("csch", Type::TEXT)],
        &common::rows(&[&[Some(database), Some("public")]]),
        true,
    )
    .await
}

/// The same read through a bare and through an explicitly `public.`-qualified
/// relation; both must resolve to the connected database's rows.
async fn assert_mapping(
    client: &Client,
    id_unqualified: &str,
    id_qualified: &str,
    bug: &str,
    expected: &Rows,
) -> Result<()> {
    common::assert_query(
        client,
        id_unqualified,
        bug,
        "SELECT id, origin FROM mapping_items ORDER BY id",
        &[("id", Type::INT4), ("origin", Type::TEXT)],
        expected,
        true,
    )
    .await?;
    common::assert_query(
        client,
        id_qualified,
        bug,
        "SELECT id, origin FROM public.mapping_items ORDER BY id",
        &[("id", Type::INT4), ("origin", Type::TEXT)],
        expected,
        true,
    )
    .await
}

/// `pg_database` must list both controlled fixture namespaces exactly once.
/// The list is scoped to those two names because every parallel suite shares
/// one metadata server and adds its own namespaces.
async fn assert_pg_database(
    client: &Client,
    id: &str,
    bug: &str,
    primary: &str,
    peer: &str,
) -> Result<()> {
    let mut names = [primary, peer];
    names.sort_unstable();
    let sql = format!(
        "SELECT datname FROM pg_catalog.pg_database \
         WHERE datname IN ({}, {}) ORDER BY datname",
        literal(primary),
        literal(peer)
    );
    common::assert_query(
        client,
        id,
        bug,
        &sql,
        &[("datname", Type::TEXT)],
        &common::rows(&[&[Some(names[0])], &[Some(names[1])]]),
        true,
    )
    .await
}

/// `pg_namespace` is scoped to the current database: only `public` and the
/// virtual `pg_catalog`. Neither namespace name may appear as a schema.
async fn assert_pg_namespace(client: &Client, id: &str, bug: &str) -> Result<()> {
    let sql = "SELECT nspname FROM pg_catalog.pg_namespace ORDER BY nspname";
    common::assert_query(
        client,
        id,
        bug,
        sql,
        &[("nspname", Type::TEXT)],
        &common::rows(&[&[Some("pg_catalog")], &[Some("public")]]),
        true,
    )
    .await
}

/// User relations discovered through the `pg_class`/`pg_namespace` join.
async fn assert_pg_class(
    client: &Client,
    id: &str,
    bug: &str,
    expected: &Rows,
) -> Result<()> {
    common::assert_query(
        client,
        id,
        bug,
        "SELECT c.relname, c.relkind FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'public' ORDER BY c.relname",
        &[("relname", Type::TEXT), ("relkind", Type::TEXT)],
        expected,
        true,
    )
    .await
}

/// Resolve every `public` relation's catalog OID back to its ordered columns.
/// Listing alone cannot tell two databases' relations apart; this join proves
/// the OIDs used by column discovery point at this database's definitions.
async fn assert_pg_attribute(
    client: &Client,
    id: &str,
    bug: &str,
    expected: &Rows,
) -> Result<()> {
    common::assert_query(
        client,
        id,
        bug,
        "SELECT c.relname, a.attname FROM pg_catalog.pg_attribute a \
         JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'public' \
         ORDER BY c.relname, a.attnum",
        &[("relname", Type::TEXT), ("attname", Type::TEXT)],
        expected,
        true,
    )
    .await
}

pub async fn run(h: &Harness) -> Result<()> {
    let primary = h.lake_namespace.clone();
    let peer = h.catalog_peer_namespace()?.to_owned();
    ensure!(
        primary != peer,
        "catalog harness exposed the same name for the primary and peer namespace: {primary}"
    );
    let primary_rows = primary_mapping_rows();
    let peer_rows = peer_mapping_rows();

    // 1. The primary connection's identity and both relation spellings.
    assert_identity(
        &h.lake,
        "catalog_mapping_identity_primary",
        BUG_IDENTITY,
        &primary,
    )
    .await?;
    assert_mapping(
        &h.lake,
        "catalog_mapping_unqualified_primary",
        "catalog_mapping_qualified_primary",
        BUG_MAPPING_ROWS,
        &primary_rows,
    )
    .await?;

    // 2. A real second database through the PG protocol.
    let peer_client = h
        .connect_database(&peer)
        .await
        .with_context(|| format!("connect a fresh client to peer database {peer}"))?;
    assert_identity(
        &peer_client,
        "catalog_mapping_identity_peer",
        BUG_IDENTITY,
        &peer,
    )
    .await?;
    assert_mapping(
        &peer_client,
        "catalog_mapping_unqualified_peer",
        "catalog_mapping_qualified_peer",
        BUG_MAPPING_ROWS,
        &peer_rows,
    )
    .await?;

    // 3. Returning to the primary connection after the peer was used must not
    //    have inherited the peer's identity through the shared factory.
    assert_identity(
        &h.lake,
        "catalog_mapping_identity_primary_after_peer",
        BUG_FACTORY_LEAK,
        &primary,
    )
    .await?;
    assert_mapping(
        &h.lake,
        "catalog_mapping_unqualified_primary_after_peer",
        "catalog_mapping_qualified_primary_after_peer",
        BUG_FACTORY_LEAK,
        &primary_rows,
    )
    .await?;

    // 4. A brand-new connection to the primary database resolves the primary
    //    identity from its own startup parameters.
    let fresh_primary = h.connect_database(&primary).await.with_context(|| {
        format!("connect a fresh client to primary database {primary}")
    })?;
    assert_identity(
        &fresh_primary,
        "catalog_mapping_identity_fresh_primary_after_peer",
        BUG_FACTORY_LEAK,
        &primary,
    )
    .await?;
    assert_mapping(
        &fresh_primary,
        "catalog_mapping_unqualified_fresh_primary_after_peer",
        "catalog_mapping_qualified_fresh_primary_after_peer",
        BUG_FACTORY_LEAK,
        &primary_rows,
    )
    .await?;

    // 5. Both namespaces are discoverable as databases from either database.
    assert_pg_database(
        &h.lake,
        "catalog_mapping_pg_database_primary",
        BUG_PG_DATABASE,
        &primary,
        &peer,
    )
    .await?;
    assert_pg_database(
        &peer_client,
        "catalog_mapping_pg_database_peer",
        BUG_PG_DATABASE,
        &primary,
        &peer,
    )
    .await?;
    // 6. Namespaces are databases, not schemas: only public/pg_catalog exist.
    assert_pg_namespace(
        &peer_client,
        "catalog_mapping_pg_namespace_peer",
        BUG_PG_NAMESPACE,
    )
    .await?;
    assert_pg_namespace(
        &h.lake,
        "catalog_mapping_pg_namespace_primary",
        BUG_PG_NAMESPACE,
    )
    .await?;

    // 7. Relation discovery is scoped to the connected database's public.
    assert_pg_class(
        &peer_client,
        "catalog_mapping_pg_class_public_peer",
        BUG_PG_CLASS,
        &peer_relations(),
    )
    .await?;
    assert_pg_class(
        &h.lake,
        "catalog_mapping_pg_class_public_primary",
        BUG_PG_CLASS,
        &primary_relations(),
    )
    .await?;

    // 8. The relation OIDs in that listing resolve to real columns.
    assert_pg_attribute(
        &peer_client,
        "catalog_mapping_pg_attribute_peer",
        BUG_PG_ATTRIBUTE,
        &peer_columns(),
    )
    .await?;
    assert_pg_attribute(
        &h.lake,
        "catalog_mapping_pg_attribute_primary",
        BUG_PG_ATTRIBUTE,
        &primary_columns(),
    )
    .await
}
