struct Dummy;

mod mysql;

mod federation;

#[cfg(test)]
mod tests {
    use adbc_core::options::{AdbcVersion, OptionDatabase};
    use adbc_core::{Connection, Database, Driver, LOAD_FLAG_DEFAULT, Statement};
    use adbc_driver_manager::ManagedDriver;
    use arrow::array::RecordBatch;
    use arrow::util::pretty;
    #[test]
    fn test() {
        let mut driver = ManagedDriver::load_from_name(
            "mysql",
            None,
            AdbcVersion::default(),
            LOAD_FLAG_DEFAULT,
            None,
        )
        .expect("Failed to load driver");

        let opts = [(
            OptionDatabase::Uri,
            "root:root@tcp(localhost:3306)/flink_test".into(),
        )];
        let db = driver
            .new_database_with_opts(opts)
            .expect("Failed to create database handle");

        let mut conn = db.new_connection().expect("Failed to create connection");

        let mut statement: adbc_driver_manager::ManagedStatement =
            conn.new_statement().unwrap();
        statement
            .set_sql_query("select * from wide_table limit 10")
            .unwrap();
        let reader = statement.execute().unwrap();
        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();

        pretty::print_batches(&batches).expect("Failed to print batches");
    }
}
