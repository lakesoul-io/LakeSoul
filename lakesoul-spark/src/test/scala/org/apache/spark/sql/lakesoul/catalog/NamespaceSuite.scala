package org.apache.spark.sql.lakesoul.catalog

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.lakesoul.test.LakeSoulSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.BeforeAndAfterEach

class NamespaceSuite extends QueryTest
  with SharedSparkSession with BeforeAndAfterEach
  with LakeSoulSQLCommandTest {

  import testImplicits._

  test("It should use ParquetScan when reading table without hash partition") {
    spark.sql("SHOW NAMESPACES")
    spark.sql("SHOW CURRENT NAMESPACE").show()
    //        spark.sql("CREATE NAMESPACE test")
    LakeSoulCatalog.createNamespace(Array("test"))
            spark.sql("USE NAMESPACE test")
    LakeSoulCatalog.useNamespace(Array("test"))
    //        spark.sql("SHOW CURRENT NAMESPACE")
    println("showCurrentNamespace=" + LakeSoulCatalog.showCurrentNamespace().head)

  }
}