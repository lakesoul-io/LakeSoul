use crate::test_utils::TestServer;

mod test_utils {
    use assert_cmd::cargo::CommandCargoExt;
    use std::process::{Child, Command};

    pub struct TestServer {
        process: Child,
    }

    impl TestServer {
        pub fn new(args: &[&'static str]) -> Self {
            // 启动服务器进程
            let process = Command::cargo_bin("lakesoul_arrow_flight_sql_server")
                .unwrap()
                .args(args)
                .spawn()
                .unwrap();

            Self { process }
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.process.kill().unwrap();
            self.process.wait().unwrap();
            println!("test server dropped");
        }
    }
}

#[test]
fn flight() {
    let server = TestServer::new(&["-h"]);
    println!("hello flight");
}
