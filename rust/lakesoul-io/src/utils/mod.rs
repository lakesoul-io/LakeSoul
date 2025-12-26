use rand::{Rng, distr::Alphanumeric};

pub fn random_str(len: usize) -> String {
    rand::rng()
        .sample_iter(Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_str_test() {
        let str = random_str(10);
        println!("{str}");
        assert_eq!(str.len(), 10);
    }
}
