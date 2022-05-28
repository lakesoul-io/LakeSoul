/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dmetasoul.lakesoul.sql

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.lakesoul.rules._

/**
  * An extension for Spark SQL.
  *
  * Scala example to create a `SparkSession`:
  * {{{
  *    import org.apache.spark.sql.SparkSession
  *
  *    val spark = SparkSession
  *       .builder()
  *       .appName("...")
  *       .master("...")
  *       .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  *       .getOrCreate()
  * }}}
  *
  * Java example to create a `SparkSession`:
  * {{{
  *    import org.apache.spark.sql.SparkSession;
  *
  *    SparkSession spark = SparkSession
  *                 .builder()
  *                 .appName("...")
  *                 .master("...")
  *                 .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  *                 .getOrCreate();
  * }}}
  *
  * Python example to create a `SparkSession`(PySpark doesn't pick up the
  * SQL conf "spark.sql.extensions" in Apache Spark 2.4.x, hence we need to activate it manually in
  * 2.4.x. However, because `SparkSession` has been created and everything has been materialized, we
  * need to clone a new session to trigger the initialization. See SPARK-25003):
  * {{{
  *    from pyspark.sql import SparkSession
  *
  *    spark = SparkSession \
  *        .builder \
  *        .appName("...") \
  *        .master("...") \
  *        .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension") \
  *        .getOrCreate()
  *    if spark.sparkContext().version < "3.":
  *        spark.sparkContext()._jvm.LakeSoulSparkSessionExtension() \
  *            .apply(spark._jsparkSession.extensions())
  *        spark = SparkSession(spark.sparkContext(), spark._jsparkSession.cloneSession())
  * }}}
  *
  * @since 0.4.0
  */

class LakeSoulSparkSessionExtension extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    //    extensions.injectParser { (session, parser) =>
    //      new LakeSoulSqlParser(parser)
    //    }


    extensions.injectResolutionRule { session =>
      ExtractMergeOperator(session)
    }

    extensions.injectResolutionRule { session =>
      LakeSoulAnalysis(session, session.sessionState.conf)
    }

    extensions.injectCheckRule { session =>
      LakeSoulUnsupportedOperationsCheck(session)
    }

    extensions.injectCheckRule { session =>
      NonMergeOperatorUDFCheck(session)
    }

    extensions.injectPostHocResolutionRule { session =>
      PreprocessTableUpdate(session.sessionState.conf)
    }

    extensions.injectPostHocResolutionRule { session =>
      PreprocessTableMergeInto(session.sessionState.conf)
    }

    extensions.injectPostHocResolutionRule { session =>
      PreprocessTableUpsert(session.sessionState.conf)
    }

    extensions.injectPostHocResolutionRule { session =>
      PreprocessTableDelete(session.sessionState.conf)
    }

    extensions.injectPostHocResolutionRule { session =>
      LakeSoulPostHocAnalysis(session)
    }

    extensions.injectResolutionRule { session =>
      ProcessCDCTableMergeOnRead(session.sessionState.conf)
    }

    extensions.injectPlannerStrategy { session =>
      SetPartitionAndOrdering(session)
    }


  }
}
