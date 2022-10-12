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

package org.apache.spark.sql.lakesoul

import java.util.{HashMap, Locale}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.utils.TableInfo


case class LakeSoulConfig[T](key: String,
                             defaultValue: String,
                             fromString: String => T,
                             validationFunction: T => Boolean,
                             helpMessage: String) {
  /**
    * Recover the saved value of this configuration from `TableInfo` or return the default if this
    * value hasn't been changed.
    */
  def fromTableInfo(table_info: TableInfo): T = {
    fromString(table_info.configuration.getOrElse(key, defaultValue))
  }

  /** Validate the setting for this configuration */
  private def validate(value: String): Unit = {
    val onErrorMessage = s"$key $helpMessage"
    try {
      require(validationFunction(fromString(value)), onErrorMessage)
    } catch {
      case e: NumberFormatException =>
        throw new IllegalArgumentException(onErrorMessage, e)
    }
  }

  /**
    * Validate this configuration and return the key - value pair to save into the metadata.
    */
  def apply(value: String): (String, String) = {
    validate(value)
    key -> value
  }
}


/**
  * Contains list of reservoir configs and validation checks.
  */
object LakeSoulConfig extends Logging {


  /**
    * A global default value set as a SQLConf will overwrite the default value of a LakeSoulConfig.
    * For example, user can run:
    * set spark.databricks.delta.properties.defaults.randomPrefixLength = 5
    * This setting will be populated to a LakeSoulTableRel during its creation time and overwrites
    * the default value of delta.randomPrefixLength.
    *
    * We accept these SQLConfs as strings and only perform validation in LakeSoulConfig. All the
    * LakeSoulConfigs set in SQLConf should adopt the same prefix.
    */
  val sqlConfPrefix = "spark.dmetasoul.lakesoul.properties.defaults."

  private val entries = new HashMap[String, LakeSoulConfig[_]]

  private def buildConfig[T](key: String,
                             defaultValue: String,
                             fromString: String => T,
                             validationFunction: T => Boolean,
                             helpMessage: String): LakeSoulConfig[T] = {
    val lakeSoulConfig = LakeSoulConfig(s"lakesoul.$key",
      defaultValue,
      fromString,
      validationFunction,
      helpMessage)
    entries.put(key.toLowerCase(Locale.ROOT), lakeSoulConfig)
    lakeSoulConfig
  }

  /**
    * Validates specified configurations and returns the normalized key -> value map.
    */
  def validateConfigurations(configurations: Map[String, String]): Map[String, String] = {
    configurations.map {
      case (key, value) if key.toLowerCase(Locale.ROOT).startsWith("lakesoul.") =>
        Option(entries.get(key.toLowerCase(Locale.ROOT).stripPrefix("lakesoul.")))
          .map(_ (value))
          .getOrElse {
            throw LakeSoulErrors.unknownConfigurationKeyException(key)
          }
      case keyvalue@(key, _) =>
        if (entries.containsKey(key.toLowerCase(Locale.ROOT))) {
          logInfo(
            s"""
               |You are trying to set a property the key of which is the same as lakesoul config: $key.
               |If you are trying to set a lakesoul config, prefix it with "lakesoul.", e.g. 'lakesoul.$key'.
            """.stripMargin)
        }
        keyvalue
    }
  }


  /**
    * Fetch global default values from SQLConf.
    */
  def mergeGlobalConfigs(sqlConfs: SQLConf,
                         tableConf: Map[String, String]): Map[String, String] = {
    import collection.JavaConverters._

    val globalConfs = entries.asScala.flatMap { case (key, config) =>
      val sqlConfKey = sqlConfPrefix + config.key.stripPrefix("lakesoul.")
      Option(sqlConfs.getConfString(sqlConfKey, null)) match {
        case Some(default) => Some(config(default))
        case _ => None
      }
    }

    val updatedConf = globalConfs.toMap ++ tableConf
    updatedConf
  }

  /**
    * Normalize the specified property keys if the key is for a lakesoul config.
    */
  def normalizeConfigKeys(propKeys: Seq[String]): Seq[String] = {
    propKeys.map {
      case key if key.toLowerCase(Locale.ROOT).startsWith("lakesoul.") =>
        Option(entries.get(key.toLowerCase(Locale.ROOT).stripPrefix("lakesoul.")))
          .map(_.key).getOrElse(key)
      case key => key
    }
  }

  /**
    * Normalize the specified property key if the key is for a lakesoul config.
    */
  def normalizeConfigKey(propKey: Option[String]): Option[String] = {
    propKey.map {
      case key if key.toLowerCase(Locale.ROOT).startsWith("lakesoul.") =>
        Option(entries.get(key.toLowerCase(Locale.ROOT).stripPrefix("lakesoul.")))
          .map(_.key).getOrElse(key)
      case key => key
    }
  }


  /**
    * Whether this lakesoul table is append-only. Files can't be deleted, or values can't be updated.
    */
  val IS_APPEND_ONLY: LakeSoulConfig[Boolean] = buildConfig[Boolean](
    "appendOnly",
    "false",
    _.toBoolean,
    _ => true,
    "needs to be a boolean.")


}

