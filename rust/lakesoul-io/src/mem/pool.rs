cfg_select! {
    feature = "test-utils" => {
        pub mod logged;
        pub use logged::*;
    }
    _ => {}
}
