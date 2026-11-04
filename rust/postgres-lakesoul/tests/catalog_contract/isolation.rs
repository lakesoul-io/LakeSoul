// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Cross-database isolation for the namespace-to-database mapping.
//!
//! Every LakeSoul namespace is the same-named PostgreSQL database, and the
//! only user schema inside it is `public` (`src/catalog.rs`: `PUBLIC_SCHEMA`;
//! `src/session.rs`: the connection validation and the default catalog/schema
//! selection). Namespaces are deliberately *not* schemas: `PgLakeSoulCatalog`
//! answers `public` plus `pg_catalog` only, and every other visible namespace
//! is an `EmptyDatabaseCatalog` marker that exists so `pg_database` can list
//! it without exposing objects.
//!
//! These are LakeSoul catalog semantics, not PostgreSQL equivalence. The
//! reference connection's management database and per-suite schema do not
//! model this mapping; expected rows come from the independent fixture inputs.
//!
//! Rejection SQLSTATE evidence: LakeSoul installs no catalog-resolution error
//! mapping. Table/schema/catalog resolution runs through DataFusion planning,
//! and `datafusion-postgres` reports those failures as `PgWireError::ApiError`
//! (`src/handlers.rs`: `statement_to_plan` in the simple and the extended
//! Parse path). `pgwire-0.41.1/src/error.rs` maps `ApiError` to
//! `ERROR XX000`. An unknown `dbname` is refused during startup by
//! `fatal_startup_error` (`src/server.rs`) as `FATAL XX000`. These exact
//! current codes are support boundaries, not claims of PG error-code parity.

use anyhow::{Context, Result, bail, ensure};
use tokio_postgres::Client;
use tokio_postgres::types::Type;

use crate::common::{assert_query, assert_rejected, rows};
use crate::support::{Harness, compare};

/// Exact SQLSTATE LakeSoul reports for catalog resolution failures and for a
/// rejected startup database (see the module documentation).
const CATALOG_ERROR_SQLSTATE: &str = "XX000";

/// Contract label attached to helper diagnostics; the case id stays separate.
const CONTRACT: &str = "catalog-namespace-database-isolation";

/// Rows of `public.mapping_items` in the primary namespace.
fn alpha_rows() -> compare::Rows {
    rows(&[&[Some("101"), Some("alpha")], &[Some("102"), Some("alpha")]])
}

/// Rows of `public.mapping_items` in the peer namespace.
fn beta_rows() -> compare::Rows {
    rows(&[&[Some("201"), Some("beta")], &[Some("202"), Some("beta")]])
}

/// `mapping_items(id INTEGER, origin TEXT)` projection.
fn mapping_columns() -> [(&'static str, Type); 2] {
    [("id", Type::INT4), ("origin", Type::TEXT)]
}

/// Run the isolation cases against the harness's primary and peer databases.
pub async fn run(h: &Harness) -> Result<()> {
    let primary = h.lake_namespace.clone();
    let peer = h.catalog_peer_namespace()?.to_owned();
    ensure!(
        primary != peer,
        "case=iso-harness identities overlap: primary namespace {primary:?} equals the peer namespace"
    );

    // Both connections are live at the same time: a leaked object in one
    // namespace cannot be hidden by reconnecting between the reads.
    let peer_client = h.connect_database(&peer).await.with_context(|| {
        format!(
            "case=iso-peer-connect dbname={peer:?} expected a live connection to the \
             peer namespace on the same server"
        )
    })?;

    own_rows_are_database_local(h, &peer_client, &primary, &peer).await?;
    cross_database_references_are_rejected(h, &peer_client, &primary, &peer).await?;
    search_path_cannot_bypass_database(h, &peer_client, &peer).await?;
    unknown_database_startup_is_rejected(h, &peer_client).await?;

    drop(peer_client);
    eprintln!(
        "catalog-isolation case=iso complete primary={primary} peer={peer} \
         sqlstate={CATALOG_ERROR_SQLSTATE} classification=support-boundary"
    );
    Ok(())
}

/// Positive controls: three-part qualification works for the current database;
/// the mapping module already checks bare and `public.`-qualified lookups.
///
/// These must pass before the rejections below mean anything; otherwise a
/// rejected cross-database read could be a generic "nothing is readable".
async fn own_rows_are_database_local(
    h: &Harness,
    peer_client: &Client,
    primary: &str,
    peer: &str,
) -> Result<()> {
    let columns = mapping_columns();
    let alpha = alpha_rows();
    let beta = beta_rows();

    // Primary namespace, three-part `database.public.table` with its own
    // database name: qualification is supported for the current database.
    assert_query(
        &h.lake,
        "iso-own-catalog-primary",
        CONTRACT,
        &format!("SELECT id, origin FROM \"{primary}\".public.mapping_items ORDER BY id"),
        &columns,
        &alpha,
        true,
    )
    .await?;

    assert_query(
        peer_client,
        "iso-own-catalog-peer",
        CONTRACT,
        &format!("SELECT id, origin FROM \"{peer}\".public.mapping_items ORDER BY id"),
        &columns,
        &beta,
        true,
    )
    .await?;

    Ok(())
}

/// Negative controls: another namespace is reachable neither as a schema name
/// nor as a foreign catalog, and a table that exists only in the other
/// database is not visible even through the local `public` schema.
///
/// `assert_rejected` runs both the simple query and the extended Parse path,
/// asserts the exact server SQLSTATE, and then performs the first subsequent
/// read of this connection's own `public.mapping_items`; a rejected statement
/// must leave the connection able to answer with its own rows.
async fn cross_database_references_are_rejected(
    h: &Harness,
    peer_client: &Client,
    primary: &str,
    peer: &str,
) -> Result<()> {
    // Primary connection: peer namespace used as a schema name (2-part).
    assert_rejected(
        &h.lake,
        "iso-cross-schema-primary",
        &format!("SELECT id, origin FROM \"{peer}\".mapping_items ORDER BY id"),
        CATALOG_ERROR_SQLSTATE,
        &alpha_rows(),
    )
    .await?;
    // Primary connection: peer namespace used as a foreign catalog (3-part).
    // The current-database positive above proves three-part qualification is
    // otherwise supported, so this rejection targets the foreign catalog.
    assert_rejected(
        &h.lake,
        "iso-cross-catalog-primary",
        &format!("SELECT id, origin FROM \"{peer}\".public.mapping_items ORDER BY id"),
        CATALOG_ERROR_SQLSTATE,
        &alpha_rows(),
    )
    .await?;
    // Primary connection: a table that exists only in the peer database.
    assert_rejected(
        &h.lake,
        "iso-foreign-unqualified-primary",
        "SELECT id FROM only_b",
        CATALOG_ERROR_SQLSTATE,
        &alpha_rows(),
    )
    .await?;
    assert_rejected(
        &h.lake,
        "iso-foreign-public-primary",
        "SELECT id FROM public.only_b",
        CATALOG_ERROR_SQLSTATE,
        &alpha_rows(),
    )
    .await?;

    // Peer connection: the mirror image, with the primary namespace and the
    // primary-only table.
    assert_rejected(
        peer_client,
        "iso-cross-schema-peer",
        &format!("SELECT id, origin FROM \"{primary}\".mapping_items ORDER BY id"),
        CATALOG_ERROR_SQLSTATE,
        &beta_rows(),
    )
    .await?;
    assert_rejected(
        peer_client,
        "iso-cross-catalog-peer",
        &format!("SELECT id, origin FROM \"{primary}\".public.mapping_items ORDER BY id"),
        CATALOG_ERROR_SQLSTATE,
        &beta_rows(),
    )
    .await?;
    assert_rejected(
        peer_client,
        "iso-foreign-unqualified-peer",
        "SELECT id FROM only_a",
        CATALOG_ERROR_SQLSTATE,
        &beta_rows(),
    )
    .await?;
    assert_rejected(
        peer_client,
        "iso-foreign-public-peer",
        "SELECT id FROM public.only_a",
        CATALOG_ERROR_SQLSTATE,
        &beta_rows(),
    )
    .await?;

    eprintln!(
        "PASS case=iso-cross-schema|iso-cross-catalog|iso-foreign-unqualified|\
         iso-foreign-public primary+peer sqlstate={CATALOG_ERROR_SQLSTATE} recovery=own-rows"
    );
    Ok(())
}

/// `search_path` cannot alias another namespace as a schema, cannot bypass the
/// database boundary, and cannot leak into another connection.
///
/// A dedicated primary-namespace connection owns the mutation; it is dropped
/// afterwards, so no shared client is left with a non-default path. The
/// documented default (`public` resolution, `SessionSettings::default`) is
/// checked through the untouched `h.lake`.
///
/// Scope: this only proves no-bypass and default-`public` behavior. It does
/// not claim arbitrary multi-schema resolution, `current_schema` behavior
/// under a non-default path, or namespace-to-schema mapping.
async fn search_path_cannot_bypass_database(
    h: &Harness,
    peer_client: &Client,
    peer: &str,
) -> Result<()> {
    let scoped = h.connect_lake().await.context(
        "case=iso-search-path SQL=connect dbname=primary expected a dedicated \
         primary connection for the search_path mutation",
    )?;
    let primary_default =
        show_search_path(&h.lake, "iso-search-path-primary-default").await?;
    let peer_default =
        show_search_path(peer_client, "iso-search-path-peer-default").await?;

    // A path whose first entry is another namespace, then `public`. If the
    // namespace were treated as a schema, `peer` rows would win here.
    let path = format!("{peer}, public");
    scoped
        .batch_execute(&format!("SET search_path = '{path}'"))
        .await
        .with_context(|| {
            format!("case=iso-search-path SQL=SET search_path = '{path}' expected SET to be accepted")
        })?;

    // No silent positive no-op: the SET must actually be recorded and visible
    // on this connection, even though it does not change resolution.
    let observed = show_search_path(&scoped, "iso-search-path-set").await?;
    ensure!(
        observed == path,
        "case=iso-search-path SQL=SET search_path = '{path}' expected SHOW search_path \
         to report {path:?}, actual {observed:?}"
    );

    let columns = mapping_columns();
    // Unqualified resolution still belongs to this database's `public`.
    assert_query(
        &scoped,
        "iso-search-path-primary-unqualified",
        CONTRACT,
        "SELECT id, origin FROM mapping_items ORDER BY id",
        &columns,
        &alpha_rows(),
        true,
    )
    .await?;
    // Foreign-only tables stay invisible after the path change, both
    // unqualified and through the local `public` schema.
    assert_rejected(
        &scoped,
        "iso-search-path-foreign-unqualified",
        "SELECT id FROM only_b",
        CATALOG_ERROR_SQLSTATE,
        &alpha_rows(),
    )
    .await?;
    assert_rejected(
        &scoped,
        "iso-search-path-foreign-public",
        "SELECT id FROM public.only_b",
        CATALOG_ERROR_SQLSTATE,
        &alpha_rows(),
    )
    .await?;
    // The peer connection is untouched: same rows through default and
    // `public`, and its own `search_path` value did not inherit the mutation.
    assert_query(
        peer_client,
        "iso-search-path-peer-unqualified",
        CONTRACT,
        "SELECT id, origin FROM mapping_items ORDER BY id",
        &columns,
        &beta_rows(),
        true,
    )
    .await?;
    assert_query(
        peer_client,
        "iso-search-path-peer-public",
        CONTRACT,
        "SELECT id, origin FROM public.mapping_items ORDER BY id",
        &columns,
        &beta_rows(),
        true,
    )
    .await?;
    let peer_after = show_search_path(peer_client, "iso-search-path-peer-after").await?;
    ensure!(
        peer_after == peer_default,
        "case=iso-search-path peer connection: expected search_path {peer_default:?} to be \
         unchanged, actual {peer_after:?}"
    );
    ensure!(
        peer_after != path,
        "case=iso-search-path peer connection inherited the primary connection's \
         search_path {path:?}"
    );
    // The shared primary connection (never mutated) is unchanged as well.
    let primary_after =
        show_search_path(&h.lake, "iso-search-path-primary-unchanged").await?;
    ensure!(
        primary_after == primary_default,
        "case=iso-search-path primary connection: expected search_path \
         {primary_default:?} to be unchanged, actual {primary_after:?}"
    );

    drop(scoped);
    eprintln!(
        "PASS case=iso-search-path set={path} primary-rows=[101,102] peer-rows=[201,202] \
         foreign-only=rejected sqlstate={CATALOG_ERROR_SQLSTATE} isolation=connection-local"
    );
    Ok(())
}

/// Read `SHOW search_path` as one text cell.
///
/// The SET/SHOW hook stores the assigned string in connection metadata and
/// returns it verbatim (`datafusion-postgres/src/hooks/set_show.rs`), so this
/// is a real check that the SET reached this connection.
async fn show_search_path(client: &Client, case: &str) -> Result<String> {
    let context = format!("case={case} SQL=SHOW search_path protocol=simple-text");
    let result = compare::simple(client, "SHOW search_path")
        .await
        .with_context(|| context.clone())?;
    ensure!(
        result.names == vec!["search_path".to_string()],
        "{context}: expected column search_path, actual {:?}",
        result.names
    );
    ensure!(
        result.rows.len() == 1 && result.rows[0].len() == 1,
        "{context}: expected one cell, actual rows {:?}",
        result.rows
    );
    result.rows[0][0]
        .clone()
        .with_context(|| format!("{context}: SHOW search_path returned NULL"))
}

/// A `dbname` that is not a visible namespace is refused at startup.
///
/// `PgSessionFactory::create_session` rejects the identity before a session
/// exists, and the startup handler turns that into a `FATAL` error
/// (`fatal_startup_error`). The client must observe that server error (not a
/// local driver failure), and both already-connected clients must remain
/// healthy and continue reading their own namespace afterwards.
async fn unknown_database_startup_is_rejected(
    h: &Harness,
    peer_client: &Client,
) -> Result<()> {
    let unknown = format!("{}_missing_database", h.lake_namespace);
    let case = "iso-unknown-database-startup";
    let error = match h.connect_database(&unknown).await {
        Ok(_) => bail!(
            "case={case} dbname={unknown:?} expected a startup rejection, actual live connection"
        ),
        Err(error) => error,
    };

    // The harness must preserve the driver error as a source; a stringified
    // message cannot prove the SQLSTATE came from the server.
    let db = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<tokio_postgres::Error>())
        .and_then(tokio_postgres::Error::as_db_error)
        .with_context(|| {
            format!(
                "case={case} dbname={unknown:?} expected the server ErrorResponse preserved \
                 in the connect error chain (source: src/server.rs fatal_startup_error), \
                 actual error: {error:#}"
            )
        })?;
    ensure!(
        db.code().code() == CATALOG_ERROR_SQLSTATE,
        "case={case} dbname={unknown:?} expected SQLSTATE {CATALOG_ERROR_SQLSTATE}, \
         actual {} ({})",
        db.code().code(),
        db.message()
    );
    ensure!(
        db.severity() == "FATAL",
        "case={case} dbname={unknown:?} expected severity FATAL, actual {} ({})",
        db.severity(),
        db.message()
    );

    // The failed connect returns no client/driver pair; existing sessions must
    // still answer from their own databases, not a partially selected identity.
    assert_query(
        &h.lake,
        "iso-unknown-database-primary-live",
        CONTRACT,
        "SELECT id, origin FROM public.mapping_items ORDER BY id",
        &mapping_columns(),
        &alpha_rows(),
        true,
    )
    .await?;
    assert_query(
        peer_client,
        "iso-unknown-database-peer-live",
        CONTRACT,
        "SELECT id, origin FROM public.mapping_items ORDER BY id",
        &mapping_columns(),
        &beta_rows(),
        true,
    )
    .await?;

    eprintln!(
        "PASS case={case} dbname={unknown} sqlstate={CATALOG_ERROR_SQLSTATE} severity=FATAL \
         primary-connection=live peer-connection=live"
    );
    Ok(())
}
