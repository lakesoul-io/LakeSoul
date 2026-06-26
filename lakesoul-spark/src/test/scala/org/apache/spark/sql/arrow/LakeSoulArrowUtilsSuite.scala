// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.arrow

import java.util

import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class LakeSoulArrowUtilsSuite extends AnyFunSuite {

  private val marker = "__lakesoul_arrow_field__"

  test("large offset types are lossless in metadata and regular in execution") {
    val largeText = field("text", ArrowType.LargeUtf8.INSTANCE)
    val largeBinary = field("bytes", ArrowType.LargeBinary.INSTANCE)
    val listElement = field(
      "item",
      ArrowType.LargeUtf8.INSTANCE,
      nullable = false,
      metadata = Map("child_key" -> "child_value"))
    val largeList = field(
      "items",
      ArrowType.LargeList.INSTANCE,
      children = Seq(listElement))
    val nestedLarge = field(
      "nested",
      ArrowType.Struct.INSTANCE,
      children = Seq(field("payload", ArrowType.LargeBinary.INSTANCE)))
    val original = new Schema(Seq(largeText, largeBinary, largeList, nestedLarge).asJava)

    val sparkSchema = ArrowUtils.fromArrowSchema(original)
    assert(sparkSchema("text").dataType == StringType)
    assert(sparkSchema("bytes").dataType == BinaryType)
    assert(sparkSchema("items").dataType == ArrayType(StringType, containsNull = false))
    assert(sparkSchema.fields.forall(_.metadata.contains(marker)))

    val execution = ArrowUtils.toArrowSchema(sparkSchema)
    assert(execution.findField("text").getType == ArrowType.Utf8.INSTANCE)
    assert(execution.findField("bytes").getType == ArrowType.Binary.INSTANCE)
    assert(execution.findField("items").getType == ArrowType.List.INSTANCE)
    assert(execution.findField("items").getChildren.get(0).getType == ArrowType.Utf8.INSTANCE)
    assert(execution.findField("nested").getChildren.get(0).getType == ArrowType.Binary.INSTANCE)

    assert(ArrowUtils.toMetadataArrowSchema(sparkSchema) == original)
  }

  test("metadata restoration follows current nested Spark schema") {
    val originalElement = field(
      "item",
      ArrowType.Struct.INSTANCE,
      nullable = false,
      children = Seq(field("value", ArrowType.LargeUtf8.INSTANCE)))
    val original = new Schema(Seq(field(
      "items",
      ArrowType.LargeList.INSTANCE,
      children = Seq(originalElement),
      metadata = Map("spark_comment" -> "old comment"))).asJava)

    val loaded = ArrowUtils.fromArrowSchema(original)
    val evolvedElement = loaded("items").dataType.asInstanceOf[ArrayType]
      .elementType.asInstanceOf[StructType]
      .add("count", LongType, nullable = true)
    val evolvedField = loaded("items")
      .copy(dataType = ArrayType(evolvedElement, containsNull = false))
      .withComment("new comment")
    val restored = ArrowUtils.toMetadataArrowSchema(StructType(Seq(evolvedField)))
      .findField("items")

    assert(restored.getType == ArrowType.LargeList.INSTANCE)
    assert(restored.getMetadata.get("spark_comment") == "new comment")
    assert(restored.getChildren.get(0).getName == "item")
    assert(restored.getChildren.get(0).getChildren.get(0).getType == ArrowType.LargeUtf8.INSTANCE)
    assert(restored.getChildren.get(0).getChildren.get(1).getType == new ArrowType.Int(64, true))
  }

  test("incompatible Spark type change discards the old Arrow type hint") {
    val original = new Schema(Seq(field("value", ArrowType.LargeUtf8.INSTANCE)).asJava)
    val loaded = ArrowUtils.fromArrowSchema(original)
    val changed = StructType(Seq(loaded("value").copy(dataType = BinaryType)))

    assert(ArrowUtils.toMetadataArrowSchema(changed).findField("value").getType ==
      ArrowType.Binary.INSTANCE)
  }

  test("existing time and timestamp hints use structured restoration") {
    val original = new Schema(Seq(
      field("time", new ArrowType.Time(TimeUnit.MILLISECOND, 32)),
      field("timestamp", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null))).asJava)
    val loaded = ArrowUtils.fromArrowSchema(original)

    assert(ArrowUtils.toArrowSchema(loaded) == original)
    assert(ArrowUtils.toMetadataArrowSchema(loaded) == original)
  }

  test("invalid Arrow field marker reports the field name") {
    val metadata = new MetadataBuilder().putString(marker, "not-json").build()
    val schema = StructType(Seq(StructField("broken", StringType, metadata = metadata)))

    val error = intercept[IllegalArgumentException] {
      ArrowUtils.toMetadataArrowSchema(schema)
    }
    assert(error.getMessage.contains("broken"))
  }

  private def field(
                     name: String,
                     dataType: ArrowType,
                     nullable: Boolean = true,
                     children: Seq[Field] = Seq.empty,
                     metadata: Map[String, String] = Map.empty): Field = {
    new Field(
      name,
      new FieldType(nullable, dataType, null, new util.HashMap(metadata.asJava)),
      children.asJava)
  }
}
