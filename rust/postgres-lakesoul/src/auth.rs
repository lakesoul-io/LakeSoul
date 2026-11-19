// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! JWT authentication for PostgreSQL connections.
//!
//! With `--jwt-auth` the server asks every client for a cleartext password
//! and treats it as the LakeSoul JWT the Flight server and the S3 proxy
//! already accept: HS256, claims `{sub, group, exp}`, signed with the
//! metadata client secret. The token's `sub` must name the startup user.
//!
//! Without the flag nothing here runs, and startup is the unauthenticated
//! path it always was.
//!
//! With `--rbac` as well, the token's `group` becomes the connection's
//! [`DomainAccess`]: the LakeSoul domain whose namespaces and tables the
//! connection may see, by the rule of `script/meta_rbac_init.sql`. What the
//! connection may not see does not exist for it - "database does not exist"
//! at startup, `42P01` for a table - the way LakeSoul's row-level security
//! hides rows from the JVM engines.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use datafusion_postgres::pgwire;
use jsonwebtoken::errors::ErrorKind;
use lakesoul_metadata::{Claims, JwtServer, MetaDataClientRef};
use parking_lot::Mutex;
use pgwire::api::ClientInfo;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

/// Refused connection attempts, by reason.
const AUTH_FAILURES_TOTAL: &str = "lakesoul_auth_failures_total";
const FAILURE_REASONS: [&str; 5] = [
    "missing_token",
    "invalid_token",
    "expired",
    "user_mismatch",
    "unknown_user",
];
static DESCRIBE_METRICS: Once = Once::new();

/// Domains every connection may see, whatever its own: the two public
/// domains of `meta_rbac_init.sql`.
const PUBLIC_DOMAINS: [&str; 2] = ["public", "lake-public"];

/// How connections authenticate. The default is no authentication.
#[derive(Clone, Default)]
pub struct AuthSettings {
    /// Verifies the token sent as the password. `None` turns the password
    /// exchange off.
    pub jwt: Option<Arc<JwtServer>>,
    /// Enforce the token's domain on databases and tables. Only meaningful
    /// with `jwt`; the CLI refuses it otherwise.
    pub rbac: bool,
    /// Validates `(sub, group)` against the LakeSoul user directory. Installed
    /// only for RBAC deployments.
    pub user_directory: Option<Arc<UserDirectory>>,
}

/// A short-lived cache of the `domain_user` directory. The ACL view has a
/// five-second TTL; the same bound applies to new connections so a just-created
/// user can connect immediately and a disabled user stops on the next token.
#[derive(Debug)]
pub struct UserDirectory {
    client: MetaDataClientRef,
    entries: Mutex<HashMap<(String, String), (bool, Instant)>>,
}

impl UserDirectory {
    const TTL: Duration = Duration::from_secs(5);

    pub fn new(client: MetaDataClientRef) -> Self {
        Self {
            client,
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub async fn is_user(
        &self,
        domain: &str,
        user: &str,
    ) -> lakesoul_metadata::Result<bool> {
        let key = (domain.to_string(), user.to_string());
        if let Some((allowed, checked)) = self.entries.lock().get(&key).cloned()
            && checked.elapsed() < Self::TTL
        {
            return Ok(allowed);
        }
        let allowed = self.client.is_domain_user(domain, user).await?;
        self.entries.lock().insert(key, (allowed, Instant::now()));
        Ok(allowed)
    }
}

impl AuthSettings {
    /// Token authentication with the metadata client secret, the key every
    /// LakeSoul service derives from the same metadata credentials.
    pub fn jwt(secret: &str) -> Self {
        Self {
            jwt: Some(Arc::new(JwtServer::new(secret))),
            rbac: false,
            user_directory: None,
        }
    }

    /// Turns domain enforcement on or off.
    pub fn with_rbac(mut self, rbac: bool) -> Self {
        self.rbac = rbac;
        self
    }

    /// Install the user directory checked by `--rbac` connections.
    pub fn with_user_directory(mut self, directory: Arc<UserDirectory>) -> Self {
        self.user_directory = Some(directory);
        self
    }

    pub fn user_directory(&self) -> Option<Arc<UserDirectory>> {
        self.user_directory.clone()
    }
}

impl fmt::Debug for AuthSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthSettings")
            .field("jwt", &self.jwt.is_some())
            .field("rbac", &self.rbac)
            .field("user_directory", &self.user_directory.is_some())
            .finish()
    }
}

/// The LakeSoul domain one connection acts in, from its token's `group`.
///
/// The rule is the row-level-security rule of `meta_rbac_init.sql` without
/// role inheritance: a token carries exactly one group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainAccess {
    domain: String,
}

impl DomainAccess {
    pub fn new(domain: impl Into<String>) -> Self {
        Self {
            domain: domain.into(),
        }
    }

    /// The connection's own domain: the one its new tables are created in.
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Whether an object in `domain` is visible to this connection.
    pub fn allows(&self, domain: &str) -> bool {
        domain == self.domain || PUBLIC_DOMAINS.contains(&domain)
    }
}

/// Describes the authentication metrics and exports every reason at zero.
pub fn register_metrics() {
    DESCRIBE_METRICS.call_once(|| {
        metrics::describe_counter!(
            AUTH_FAILURES_TOTAL,
            "Connections refused during token authentication, by reason: \
             missing_token, invalid_token (malformed or wrongly signed), \
             expired, user_mismatch (token issued to another user)"
        );
    });
    for reason in FAILURE_REASONS {
        metrics::counter!(AUTH_FAILURES_TOTAL, "reason" => reason).increment(0);
    }
}

pub(crate) fn record_failure(failure: &AuthFailure) {
    metrics::counter!(AUTH_FAILURES_TOTAL, "reason" => failure.reason()).increment(1);
}

/// Refuses `what` on a connection that runs under domain enforcement.
///
/// For the paths that reach tables without the connection's schema provider
/// (subscriptions write through the metadata client; IVM maintenance can
/// refresh on sessions of its own), so they cannot apply the domain filter.
pub(crate) fn refuse_under_domain_enforcement<C>(
    client: &C,
    what: &str,
) -> PgWireResult<()>
where
    C: ClientInfo + ?Sized,
{
    let state = crate::server::connection_session(client)?;
    if state.session.identity.access.is_some() {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            "42501".to_string(),
            format!("{what} is not available while domain enforcement (--rbac) is on"),
        ))));
    }
    Ok(())
}

/// Why a password was refused. Logged on the server; the client only ever
/// sees `28P01`, so a probe cannot tell a forged token from an expired one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AuthFailure {
    /// The client sent an empty password.
    MissingToken,
    /// `exp` has passed.
    Expired,
    /// The token decodes and verifies, but names another user.
    UserMismatch { token_user: String },
    /// The token is valid but the user is absent or disabled in the directory.
    UnknownUser { user: String, group: String },
    /// Malformed, wrongly signed, or missing a required claim.
    Invalid(String),
}

impl AuthFailure {
    /// A short, stable label for logs.
    pub(crate) fn reason(&self) -> &'static str {
        match self {
            AuthFailure::MissingToken => "missing_token",
            AuthFailure::Expired => "expired",
            AuthFailure::UserMismatch { .. } => "user_mismatch",
            AuthFailure::UnknownUser { .. } => "unknown_user",
            AuthFailure::Invalid(_) => "invalid_token",
        }
    }
}

impl fmt::Display for AuthFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthFailure::MissingToken => f.write_str("no token was sent"),
            AuthFailure::Expired => f.write_str("the token has expired"),
            AuthFailure::UserMismatch { token_user } => {
                write!(f, "the token was issued to {token_user:?}")
            }
            AuthFailure::UnknownUser { user, group } => {
                write!(f, "user {user:?} is not enabled in domain {group:?}")
            }
            AuthFailure::Invalid(detail) => write!(f, "the token is invalid: {detail}"),
        }
    }
}

/// Checks `token` for the startup user `user`.
pub(crate) fn verify_token(
    jwt: &JwtServer,
    user: &str,
    token: &str,
) -> Result<Claims, AuthFailure> {
    if token.is_empty() {
        return Err(AuthFailure::MissingToken);
    }
    let claims = jwt
        .decode_token(token)
        .map_err(|error| match error.kind() {
            ErrorKind::ExpiredSignature => AuthFailure::Expired,
            _ => AuthFailure::Invalid(error.to_string()),
        })?;
    if claims.sub != user {
        return Err(AuthFailure::UserMismatch {
            token_user: claims.sub,
        });
    }
    Ok(claims)
}

/// Signs a token for `user` in `group` that expires `ttl` from now.
pub(crate) fn mint_token(
    jwt: &JwtServer,
    user: &str,
    group: &str,
    ttl: Duration,
) -> Result<String, String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| format!("system clock is before the epoch: {error}"))?;
    let exp = now
        .checked_add(ttl)
        .and_then(|exp| usize::try_from(exp.as_secs()).ok())
        .ok_or_else(|| format!("ttl {ttl:?} is too large"))?;
    jwt.create_token(&Claims {
        sub: user.to_string(),
        group: group.to_string(),
        exp,
    })
    .map_err(|error| format!("cannot sign the token: {error}"))
}

/// Parses a token lifetime: a positive number with an optional unit, `s`,
/// `m`, `h` or `d` (seconds when omitted), such as `90`, `15m` or `1h`.
pub(crate) fn parse_ttl(text: &str) -> Result<Duration, String> {
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let number: u64 = number
        .parse()
        .map_err(|_| format!("invalid ttl {text:?}: expected e.g. 90, 15m, 1h or 7d"))?;
    let scale = match unit {
        "" | "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 24 * 60 * 60,
        _ => {
            return Err(format!(
                "invalid ttl unit {unit:?} in {text:?}: use s, m, h or d"
            ));
        }
    };
    let seconds = number
        .checked_mul(scale)
        .ok_or_else(|| format!("ttl {text:?} is too large"))?;
    if seconds == 0 {
        return Err("ttl must be positive".to_string());
    }
    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server() -> JwtServer {
        JwtServer::new("unit-test-secret")
    }

    fn token_with_exp(jwt: &JwtServer, user: &str, exp: usize) -> String {
        jwt.create_token(&Claims {
            sub: user.to_string(),
            group: "team_a".to_string(),
            exp,
        })
        .expect("sign")
    }

    #[test]
    fn a_valid_token_for_the_startup_user_is_accepted() {
        let jwt = server();
        let token =
            mint_token(&jwt, "alice", "team_a", Duration::from_secs(600)).expect("mint");
        let claims = verify_token(&jwt, "alice", &token).expect("valid token");
        assert_eq!(claims.sub, "alice");
        assert_eq!(claims.group, "team_a");
    }

    #[test]
    fn each_refusal_has_its_own_reason() {
        let jwt = server();
        let token =
            mint_token(&jwt, "alice", "team_a", Duration::from_secs(600)).expect("mint");

        assert_eq!(
            verify_token(&jwt, "alice", ""),
            Err(AuthFailure::MissingToken)
        );
        assert_eq!(
            verify_token(&jwt, "bob", &token),
            Err(AuthFailure::UserMismatch {
                token_user: "alice".to_string()
            })
        );
        // Far enough in the past to clear the validator's leeway.
        let expired = token_with_exp(&jwt, "alice", 1_000_000);
        assert_eq!(
            verify_token(&jwt, "alice", &expired),
            Err(AuthFailure::Expired)
        );
        assert_eq!(
            verify_token(&jwt, "alice", "not-a-token")
                .unwrap_err()
                .reason(),
            "invalid_token"
        );
        let foreign = mint_token(
            &JwtServer::new("another-secret"),
            "alice",
            "team_a",
            Duration::from_secs(600),
        )
        .expect("mint");
        assert_eq!(
            verify_token(&jwt, "alice", &foreign).unwrap_err().reason(),
            "invalid_token",
            "a token signed with another secret must not verify"
        );
    }

    #[test]
    fn ttl_accepts_units_and_refuses_nonsense() {
        assert_eq!(parse_ttl("90"), Ok(Duration::from_secs(90)));
        assert_eq!(parse_ttl("45s"), Ok(Duration::from_secs(45)));
        assert_eq!(parse_ttl("15m"), Ok(Duration::from_secs(900)));
        assert_eq!(parse_ttl("1h"), Ok(Duration::from_secs(3600)));
        assert_eq!(parse_ttl("7d"), Ok(Duration::from_secs(7 * 86_400)));
        for bad in [
            "",
            "h",
            "0",
            "0h",
            "-1h",
            "1w",
            "1.5h",
            "99999999999999999999d",
        ] {
            assert!(parse_ttl(bad).is_err(), "{bad:?} must be refused");
        }
    }

    #[test]
    fn debug_output_never_contains_key_material() {
        let settings = AuthSettings::jwt("super-secret-value");
        let debug = format!("{settings:?}");
        assert!(!debug.contains("super-secret-value"), "{debug}");
        assert_eq!(
            debug,
            "AuthSettings { jwt: true, rbac: false, user_directory: false }"
        );
    }
}

#[cfg(test)]
mod domain_tests {
    use super::DomainAccess;

    #[test]
    fn a_domain_sees_itself_and_the_public_domains_only() {
        let access = DomainAccess::new("team_a");
        assert_eq!(access.domain(), "team_a");
        assert!(access.allows("team_a"));
        assert!(access.allows("public"));
        assert!(access.allows("lake-public"));
        assert!(!access.allows("team_b"));
        assert!(!access.allows("TEAM_A"), "domains compare exactly");
        assert!(!access.allows(""));
        assert!(!access.allows("team_a_admins"));
    }

    #[test]
    fn the_public_domain_sees_only_public_objects() {
        let access = DomainAccess::new("public");
        assert!(access.allows("public"));
        assert!(access.allows("lake-public"));
        assert!(!access.allows("team_a"));
    }
}
