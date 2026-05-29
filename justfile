meta-cleanup:
     PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f script/meta_cleanup.sql lakesoul_test

spark-test-1:
    -mvn -q -B test -pl lakesoul-spark \
        -am -Pcross-build -Pparallel-test --file pom.xml \
        -Dtest='!ANNCase,UpdateScalaSuite,AlterTableByNameSuite,ReadSuite,UpdateSQLSuite,ParquetNativeFilterSuite,DeleteScalaSuite,DeleteSQLSuite,ParquetV2FilterSuite,ParquetScanSuite,UpsertSuiteBase' \
        -Dsurefire.failIfNoSpecifiedTests=false \
        -Dlog4j.configurationFile=file:./lakesoul-spark/src/test/resources/log4j2-test.properties
    mvn -q surefire-report:report-only -pl lakesoul-spark -am

spark-test-2:
    -mvn -q -B test -pl lakesoul-spark \
        -am -Pcross-build -Pparallel-test --file pom.xml \
        -Dtest='!ANNCase,!UpdateScalaSuite,!AlterTableByNameSuite,!ReadSuite,!UpdateSQLSuite,!ParquetNativeFilterSuite,!DeleteScalaSuite,!DeleteSQLSuite,!ParquetV2FilterSuite,!ParquetScanSuite,!UpsertSuiteBase,!RBACOperationSuite,!DeltaJoinSuite' \
        -Dsurefire.failIfNoSpecifiedTests=false
    mvn -q surefire-report:report-only -pl lakesoul-spark -am

flink-test-1:
    -MAVEN_OPTS="-Xmx5g" mvn -q -B test -pl lakesoul-flink \
        -am -Pcross-build --file pom.xml \
        -Dtest='!LakeSoulRBACTest' \
        -Dsurefire.failIfNoSpecifiedTests=false \
        -Dlog4j.configurationFile=file:"$(pwd)/lakesoul-flink/src/test/resources/log4j2-test.properties" \
        -Dlog4j2.statusLoggerLevel=OFF
    mvn -q surefire-report:report-only -pl lakesoul-flink -am

copy-to-java ext="dylib":
  cargo build -p lakesoul-io-c -p lakesoul-metadata-c
  mkdir -p lakesoul-common/target/classes/
  cp rust/target/debug/liblakesoul_io_c.{{ext}} lakesoul-common/target/classes/
  cp rust/target/debug/liblakesoul_metadata_c.{{ext}} lakesoul-common/target/classes/
