use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;

#[async_trait]
pub trait PgCatalogContextProvider: Clone + Debug + Send + Sync + 'static {
    // retrieve all database role names
    async fn roles(&self) -> Vec<String> {
        vec![]
    }

    // retrieve database role information
    async fn role(&self, _name: &str) -> Option<Role> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct EmptyContextProvider;

impl PgCatalogContextProvider for EmptyContextProvider {}

#[async_trait]
impl<T> PgCatalogContextProvider for Arc<T>
where
    T: PgCatalogContextProvider,
{
    // retrieve all database role names
    async fn roles(&self) -> Vec<String> {
        self.roles().await
    }

    // retrieve database role information
    async fn role(&self, name: &str) -> Option<Role> {
        self.role(name).await
    }
}

/// User information stored in the authentication system
#[derive(Debug, Clone)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub roles: Vec<String>,
    pub is_superuser: bool,
    pub can_login: bool,
    pub connection_limit: Option<i32>,
}

/// Permission types for granular access control
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Permission {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    Index,
    References,
    Trigger,
    Execute,
    Usage,
    Connect,
    Temporary,
    All,
}

impl Permission {
    pub fn from_string(s: &str) -> Option<Permission> {
        match s.to_uppercase().as_str() {
            "SELECT" => Some(Permission::Select),
            "INSERT" => Some(Permission::Insert),
            "UPDATE" => Some(Permission::Update),
            "DELETE" => Some(Permission::Delete),
            "CREATE" => Some(Permission::Create),
            "DROP" => Some(Permission::Drop),
            "ALTER" => Some(Permission::Alter),
            "INDEX" => Some(Permission::Index),
            "REFERENCES" => Some(Permission::References),
            "TRIGGER" => Some(Permission::Trigger),
            "EXECUTE" => Some(Permission::Execute),
            "USAGE" => Some(Permission::Usage),
            "CONNECT" => Some(Permission::Connect),
            "TEMPORARY" => Some(Permission::Temporary),
            "ALL" => Some(Permission::All),
            _ => None,
        }
    }
}

/// Resource types for access control
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ResourceType {
    Table(String),
    Schema(String),
    Database(String),
    Function(String),
    Sequence(String),
    All,
}

/// Grant entry for specific permissions on resources
#[derive(Debug, Clone)]
pub struct Grant {
    pub permission: Permission,
    pub resource: ResourceType,
    pub granted_by: String,
    pub with_grant_option: bool,
}

/// Role information for access control
#[derive(Debug, Clone)]
pub struct Role {
    pub name: String,
    pub is_superuser: bool,
    pub can_login: bool,
    pub can_create_db: bool,
    pub can_create_role: bool,
    pub can_create_user: bool,
    pub can_replication: bool,
    pub grants: Vec<Grant>,
    pub inherited_roles: Vec<String>,
}

/// Role configuration for creation
#[derive(Debug, Clone)]
pub struct RoleConfig {
    pub name: String,
    pub is_superuser: bool,
    pub can_login: bool,
    pub can_create_db: bool,
    pub can_create_role: bool,
    pub can_create_user: bool,
    pub can_replication: bool,
}
