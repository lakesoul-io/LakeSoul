//! Code from https://github.com/datafusion-contrib/datafusion-federation


// mod optimizer;
// mod plan_node;
// pub mod schema_cast;
// #[cfg(feature = "sql")]
// pub mod sql;
// mod table_provider;

// use std::{
//     fmt,
//     hash::{Hash, Hasher},
//     sync::Arc,
// };

// use datafusion::{
//     execution::session_state::{SessionState, SessionStateBuilder},
//     optimizer::{optimizer::Optimizer, OptimizerRule},
// };

// pub use optimizer::{get_table_source, FederationOptimizerRule};
// pub use plan_node::{
//     FederatedPlanNode, FederatedPlanner, FederatedQueryPlanner, FederationPlanner,
// };
// pub use table_provider::{FederatedTableProviderAdaptor, FederatedTableSource};

// pub fn default_session_state() -> SessionState {
//     let rules = default_optimizer_rules();
//     SessionStateBuilder::new()
//         .with_optimizer_rules(rules)
//         .with_query_planner(Arc::new(FederatedQueryPlanner::new()))
//         .with_default_features()
//         .build()
// }

// pub fn default_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
//     // Get the default optimizer
//     let df_default = Optimizer::new();
//     let mut default_rules = df_default.rules;

//     // Insert the FederationOptimizerRule after the ScalarSubqueryToJoin.
//     // This ensures ScalarSubquery are replaced before we try to federate.
//     let Some(pos) = default_rules
//         .iter()
//         .position(|x| x.name() == "scalar_subquery_to_join")
//     else {
//         panic!("Could not locate ScalarSubqueryToJoin");
//     };

//     // TODO: check if we should allow other optimizers to run before the federation rule.

//     let federation_rule = Arc::new(FederationOptimizerRule::new());
//     default_rules.insert(pos + 1, federation_rule);

//     default_rules
// }

// pub type FederationProviderRef = Arc<dyn FederationProvider>;
// pub trait FederationProvider: Send + Sync {
//     // Returns the name of the provider, used for comparison.
//     fn name(&self) -> &str;

//     // Returns the compute context in which this federation provider
//     // will execute a query. For example: database instance & catalog.
//     fn compute_context(&self) -> Option<String>;

//     // Returns an optimizer that can cut out part of the plan
//     // to federate it.
//     fn optimizer(&self) -> Option<Arc<Optimizer>>;
// }

// impl fmt::Display for dyn FederationProvider {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "{} {:?}", self.name(), self.compute_context())
//     }
// }

// impl PartialEq<dyn FederationProvider> for dyn FederationProvider {
//     /// Comparing name, args and return_type
//     fn eq(&self, other: &dyn FederationProvider) -> bool {
//         self.name() == other.name() && self.compute_context() == other.compute_context()
//     }
// }

// impl Hash for dyn FederationProvider {
//     fn hash<H: Hasher>(&self, state: &mut H) {
//         self.name().hash(state);
//         self.compute_context().hash(state);
//     }
// }

// impl Eq for dyn FederationProvider {}