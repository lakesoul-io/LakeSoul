use subst::Env;

use crate::RunnerLocals;

pub mod well_known {
    pub const TEST_DIR: &str = "__TEST_DIR__";
    pub const NOW: &str = "__NOW__";
    pub const DATABASE: &str = "__DATABASE__";
}

/// Substitute environment variables and special variables like `__TEST_DIR__` in SQL.
pub(crate) struct Substitution<'a> {
    runner_locals: &'a RunnerLocals,
    subst_env_vars: bool,
}

impl Substitution<'_> {
    pub fn new(runner_locals: &RunnerLocals, subst_env_vars: bool) -> Substitution<'_> {
        Substitution {
            runner_locals,
            subst_env_vars,
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("substitution failed: {0}")]
pub(crate) struct SubstError(subst::Error);

fn now_string() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("failed to get current time")
        .as_nanos()
        .to_string()
}

impl Substitution<'_> {
    pub fn substitute(&self, input: &str) -> Result<String, SubstError> {
        if self.subst_env_vars {
            subst::substitute(input, self).map_err(SubstError)
        } else {
            Ok(self.simple_replace(input))
        }
    }

    fn simple_replace(&self, input: &str) -> String {
        let mut res = input
            .replace(
                &format!("${}", well_known::TEST_DIR),
                &self.runner_locals.test_dir(),
            )
            .replace(&format!("${}", well_known::NOW), &now_string());
        for (key, value) in self.runner_locals.vars() {
            res = res.replace(&format!("${}", key), value);
        }
        res
    }
}

impl<'a> subst::VariableMap<'a> for Substitution<'a> {
    type Value = String;

    fn get(&'a self, key: &str) -> Option<Self::Value> {
        match key {
            well_known::TEST_DIR => self.runner_locals.test_dir().into(),
            well_known::NOW => now_string().into(),
            key => self
                .runner_locals
                .get_var(key)
                .cloned()
                .or_else(|| Env.get(key)),
        }
    }
}
