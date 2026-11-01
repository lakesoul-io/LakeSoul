// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! View-chain orchestration: the dependency graph, the topological refresh
//! order with cycle detection, and the refresh policy (retry, rebuild
//! fallback, dirty self-heal).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use futures::StreamExt;

use super::*;

/// The policy of a chain refresh.
#[derive(Debug, Clone)]
pub struct ChainPolicy {
    /// The incremental refresh attempts per view beyond the first one.
    pub retries: u32,
    /// The delay between two attempts.
    pub retry_backoff: Duration,
    /// Rebuild a view whose incremental refresh kept failing.  A view marked
    /// dirty is always rebuilt: its incremental state is not trusted.
    pub rebuild_on_failure: bool,
}

impl Default for ChainPolicy {
    fn default() -> Self {
        Self {
            retries: 1,
            retry_backoff: Duration::from_millis(50),
            rebuild_on_failure: true,
        }
    }
}

/// What a chain refresh did to one view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshAction {
    /// The view had no source changes.
    Unchanged,
    /// The incremental refresh advanced the view.
    Incremental,
    /// The view was recomputed from the full source state: a dirty view, a
    /// failing incremental refresh with the rebuild fallback, or a changed
    /// definition.
    Rebuilt,
}

/// One view's chain refresh result.
#[derive(Debug, Clone)]
pub struct ViewRefresh {
    /// The view id (its target table id).
    pub view_id: String,
    /// The epoch written, or `None` when the view had no changes.
    pub epoch: Option<i64>,
    /// What the refresh did.
    pub action: RefreshAction,
}

/// The registered views reachable from the roots, sources first.
struct ViewGraph {
    specs: HashMap<String, ViewSpec>,
    order: Vec<String>,
}

impl ViewGraph {
    /// Discover the views reachable from the roots (a table that is not a
    /// registered view is a leaf) and order them sources-first.
    async fn load(runtime: &IvmRuntime, roots: &[String]) -> Result<Self> {
        let metadata = runtime.metadata();
        let mut specs = HashMap::new();
        let mut edges: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut seen = HashSet::new();
        let mut queue = roots.to_vec();
        while let Some(view_id) = queue.pop() {
            if !seen.insert(view_id.clone()) {
                continue;
            }
            let Some(value) = metadata.get_view_spec(&view_id).await? else {
                continue;
            };
            let spec: ViewSpec = serde_json::from_value(value)?;
            let sources = spec
                .source_table_ids()
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>();
            queue.extend(sources.iter().cloned());
            edges.insert(view_id.clone(), sources);
            specs.insert(view_id, spec);
        }
        let order = topological_order(&edges, roots)?;
        Ok(Self { specs, order })
    }

    /// Group the topological order into dependency levels.  A view's level is
    /// one more than the deepest source view, so views in one level never
    /// depend on each other and can be refreshed concurrently.
    fn levels(&self) -> Vec<Vec<String>> {
        dependency_levels(&self.order, |view_id| {
            self.specs
                .get(view_id)
                .map(|spec| {
                    spec.source_table_ids()
                        .into_iter()
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default()
        })
    }
}

/// Group a topological order into dependency levels.  A view's level is one
/// more than the deepest source view, so views in one level never depend on
/// each other and can be refreshed concurrently.
fn dependency_levels(
    order: &[String],
    sources: impl Fn(&str) -> Vec<String>,
) -> Vec<Vec<String>> {
    let mut level_of: HashMap<String, usize> = HashMap::new();
    let mut levels: Vec<Vec<String>> = Vec::new();
    for view_id in order {
        let level = sources(view_id)
            .into_iter()
            .filter_map(|source| level_of.get(&source).copied())
            .max()
            .map_or(0, |source| source + 1);
        level_of.insert(view_id.clone(), level);
        while levels.len() <= level {
            levels.push(Vec::new());
        }
        levels[level].push(view_id.clone());
    }
    levels
}

/// Order the view edges sources-first, starting from the roots (their
/// dependency order decides the output; the remaining views follow by id).
///
/// A cycle is rejected with its path: every member reads another member's
/// output, so no refresh order can bring them up to date.
fn topological_order(
    edges: &BTreeMap<String, Vec<String>>,
    roots: &[String],
) -> Result<Vec<String>> {
    enum Mark {
        Gray,
        Black,
    }

    fn visit<'a>(
        node: &'a str,
        edges: &'a BTreeMap<String, Vec<String>>,
        marks: &mut HashMap<&'a str, Mark>,
        path: &mut Vec<&'a str>,
        order: &mut Vec<String>,
    ) -> Result<()> {
        match marks.get(node) {
            Some(Mark::Black) => return Ok(()),
            Some(Mark::Gray) => {
                let start = path.iter().position(|entry| *entry == node).unwrap_or(0);
                let mut cycle = path[start..].to_vec();
                cycle.push(node);
                return Err(report!("view chain cycle: {}", cycle.join(" -> ")));
            }
            None => {}
        }
        marks.insert(node, Mark::Gray);
        path.push(node);
        if let Some(sources) = edges.get(node) {
            for source in sources {
                if edges.contains_key(source) {
                    visit(source, edges, marks, path, order)?;
                }
            }
        }
        path.pop();
        marks.insert(node, Mark::Black);
        order.push(node.to_string());
        Ok(())
    }

    let mut marks = HashMap::new();
    let mut path = Vec::new();
    let mut order = Vec::new();
    for node in roots {
        if edges.contains_key(node) {
            visit(node, edges, &mut marks, &mut path, &mut order)?;
        }
    }
    for node in edges.keys() {
        visit(node, edges, &mut marks, &mut path, &mut order)?;
    }
    Ok(order)
}

/// The number of independent views a chain refresh may run at once.
///
/// One keeps the historical sequential order.  Values above one run views of
/// the same dependency level concurrently; the shared DataFusion pool and the
/// metadata client still bound the real resource usage.
fn view_parallelism() -> usize {
    std::env::var("LAKESOUL_IVM_VIEW_PARALLELISM")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1)
        .max(1)
}

impl IvmRuntime {
    /// Refresh a view and every view it reads, upstream first, with the
    /// default [`ChainPolicy`].
    ///
    /// Returns one entry per view in refresh order.  A view that cannot be
    /// brought up to date aborts the chain (and is marked dirty): downstream
    /// views must never read an upstream that stayed behind.
    pub async fn refresh_view_chain(&self, view_id: &str) -> Result<Vec<ViewRefresh>> {
        let roots = [view_id.to_string()];
        self.refresh_view_chains(&roots, &ChainPolicy::default())
            .await
    }

    /// Refresh a chain with an explicit policy.
    pub async fn refresh_view_chain_with(
        &self,
        view_id: &str,
        policy: &ChainPolicy,
    ) -> Result<Vec<ViewRefresh>> {
        let roots = [view_id.to_string()];
        self.refresh_view_chains(&roots, policy).await
    }

    /// Refresh every view reachable from the roots in one traversal,
    /// upstream first.  A diamond's shared upstream is refreshed once.
    pub async fn refresh_view_chains(
        &self,
        view_ids: &[String],
        policy: &ChainPolicy,
    ) -> Result<Vec<ViewRefresh>> {
        let graph = ViewGraph::load(self, view_ids).await?;
        let parallelism = view_parallelism();
        if parallelism == 1 {
            let mut refreshed = Vec::with_capacity(graph.order.len());
            for view_id in &graph.order {
                let Some(spec) = graph.specs.get(view_id) else {
                    continue;
                };
                refreshed.push(self.refresh_chain_view(view_id, spec, policy).await?);
            }
            return Ok(refreshed);
        }

        let positions = graph
            .order
            .iter()
            .enumerate()
            .map(|(index, view_id)| (view_id.as_str(), index))
            .collect::<HashMap<_, _>>();
        let mut refreshed: Vec<Option<ViewRefresh>> =
            (0..graph.order.len()).map(|_| None).collect();
        for level in graph.levels() {
            let mut results = futures::stream::iter(level)
                .map(|view_id| {
                    let spec = graph.specs.get(&view_id).cloned();
                    async move {
                        let spec = spec?;
                        Some((
                            view_id.clone(),
                            self.refresh_chain_view(&view_id, &spec, policy).await,
                        ))
                    }
                })
                .buffer_unordered(parallelism)
                .collect::<Vec<_>>()
                .await;
            results.sort_by_key(|entry| {
                entry
                    .as_ref()
                    .and_then(|(view_id, _)| positions.get(view_id.as_str()).copied())
                    .unwrap_or(usize::MAX)
            });
            for entry in results {
                let Some((view_id, result)) = entry else {
                    continue;
                };
                let index = positions[view_id.as_str()];
                refreshed[index] = Some(result?);
            }
        }
        Ok(refreshed.into_iter().flatten().collect())
    }

    /// Refresh one view of a chain: the incremental refresh with the policy
    /// retries, the rebuild fallback and the dirty self-heal.
    async fn refresh_chain_view(
        &self,
        view_id: &str,
        spec: &ViewSpec,
        policy: &ChainPolicy,
    ) -> Result<ViewRefresh> {
        let metadata = self.metadata();
        let dirty = metadata.view_status(view_id).await?.as_deref() == Some("dirty");
        let mut refreshed = None;
        let mut failure = None;
        if !dirty {
            for attempt in 0..=policy.retries {
                match self.refresh_spec(spec).await {
                    Ok(epoch) => {
                        let action = if epoch.is_some() {
                            RefreshAction::Incremental
                        } else {
                            RefreshAction::Unchanged
                        };
                        refreshed = Some((epoch, action));
                        break;
                    }
                    Err(error) => {
                        // A dropped partition or a source window that needs a
                        // snapshot needs the rebuild, not a retry.
                        let dropped = IvmRuntime::rebuild_required(&error);
                        failure = Some(error);
                        if dropped {
                            break;
                        }
                        if attempt < policy.retries && !policy.retry_backoff.is_zero() {
                            tokio::time::sleep(policy.retry_backoff).await;
                        }
                    }
                }
            }
        }
        if refreshed.is_none() && (dirty || policy.rebuild_on_failure) {
            match self.rebuild_spec(spec).await {
                Ok(epoch) => refreshed = Some((Some(epoch), RefreshAction::Rebuilt)),
                Err(rebuild_error) => {
                    metadata.set_view_status(view_id, "dirty").await?;
                    let message = match &failure {
                        Some(refresh_error) => format!(
                            "view chain: {view_id} failed to refresh \
                             ({refresh_error}) and to rebuild ({rebuild_error})"
                        ),
                        None => format!(
                            "view chain: dirty view {view_id} failed to rebuild \
                             ({rebuild_error})"
                        ),
                    };
                    metadata.set_view_error(view_id, Some(&message)).await?;
                    return Err(report!("{message}"));
                }
            }
        }
        let Some((epoch, action)) = refreshed else {
            let error = failure.expect("a failed incremental refresh was recorded");
            metadata.set_view_status(view_id, "dirty").await?;
            let message = format!("view chain: {view_id} failed to refresh ({error})");
            metadata.set_view_error(view_id, Some(&message)).await?;
            return Err(report!("{message}"));
        };
        if dirty || action == RefreshAction::Rebuilt || failure.is_some() {
            metadata.set_view_status(view_id, "active").await?;
            metadata.set_view_error(view_id, None).await?;
        }
        Ok(ViewRefresh {
            view_id: view_id.to_string(),
            epoch,
            action,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edges(entries: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
        entries
            .iter()
            .map(|(view, sources)| {
                (
                    (*view).to_string(),
                    sources.iter().map(|source| (*source).to_string()).collect(),
                )
            })
            .collect()
    }

    fn roots(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    #[test]
    fn orders_sources_first() {
        let graph = edges(&[("a", &["b"]), ("b", &["c"]), ("c", &[])]);
        assert_eq!(
            topological_order(&graph, &roots(&["a"])).unwrap(),
            ["c", "b", "a"]
        );
    }

    #[test]
    fn orders_a_diamond_with_each_view_once() {
        let graph =
            edges(&[("a", &["b", "c"]), ("b", &["d"]), ("c", &["d"]), ("d", &[])]);
        let order = topological_order(&graph, &roots(&["a"])).unwrap();
        assert_eq!(order, ["d", "b", "c", "a"]);
    }

    #[test]
    fn rejects_cycles_with_the_path() {
        let graph = edges(&[("a", &["b"]), ("b", &["a"])]);
        let error = topological_order(&graph, &roots(&["a"]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("view chain cycle"), "{error}");
        assert!(
            error.contains("a -> b -> a") || error.contains("b -> a -> b"),
            "{error}"
        );
    }

    #[test]
    fn rejects_a_self_cycle() {
        let graph = edges(&[("a", &["a"])]);
        let error = topological_order(&graph, &roots(&["a"]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("view chain cycle"), "{error}");
        assert!(error.contains("a -> a"), "{error}");
    }

    #[test]
    fn ignores_leaf_sources() {
        let graph = edges(&[("a", &["b", "base"]), ("b", &["base"])]);
        assert_eq!(
            topological_order(&graph, &roots(&["a"])).unwrap(),
            ["b", "a"]
        );
    }

    #[test]
    fn groups_independent_views_into_one_level() {
        let order = ["d", "b", "c", "a"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let levels = dependency_levels(&order, |view| match view {
            "a" => vec!["b".to_string(), "c".to_string()],
            "b" | "c" => vec!["d".to_string()],
            _ => Vec::new(),
        });
        assert_eq!(
            levels,
            vec![
                vec!["d".to_string()],
                vec!["b".to_string(), "c".to_string()],
                vec!["a".to_string()],
            ]
        );
    }

    #[test]
    fn keeps_unrelated_roots_in_one_level() {
        let order = ["a", "b", "c"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let levels = dependency_levels(&order, |_| Vec::new());
        assert_eq!(
            levels,
            vec![vec!["a".to_string(), "b".to_string(), "c".to_string()]]
        );
    }
}
