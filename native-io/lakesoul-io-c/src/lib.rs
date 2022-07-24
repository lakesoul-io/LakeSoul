use lakesoul_io::add;

#[no_mangle]
pub extern "C" fn add_c(left: i64, right: i64) -> i64 {
    add(left, right)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add_c(2, 2);
        assert_eq!(result, 4);
    }
}
