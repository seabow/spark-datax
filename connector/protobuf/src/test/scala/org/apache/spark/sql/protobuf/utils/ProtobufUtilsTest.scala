package org.apache.spark.sql.protobuf.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.protobuf.ProtobufTestBase
import org.scalatest.funsuite.AnyFunSuite

import java.util.Locale
import scala.collection.JavaConverters._

class ProtobufUtilsTest extends AnyFunSuite {
  val student1DescFile ="target/generated-test-sources/serde_suite.desc"
  val student2DescFile = "target/generated-test-sources/student2.desc"
  private val student1Desc = ProtobufUtils.readDescriptorFileContent(student1DescFile)
  private val student2Desc = ProtobufUtils.readDescriptorFileContent(student2DescFile)
//  test("testBuildDescriptor") {
//      val schemas=( 1 to 6).map{i=>
//      val descriptor=ProtobufUtils.buildDescriptor(s"connector/protobuf/src/test/test_data/descriptor${i}","SceneSnapshotList")
//        val dataType=  SchemaConverters.toSqlType(descriptor).dataType
//        println(dataType.sql)
//        dataType
//        }
//    println(schemas.reduceOption(ProtobufSchemaUtils.merge).get.sql)
//  }

  test("get sql schema by class"){
    val messageDescriptor=ProtobufUtils.buildDescriptor(student1Desc,"SerdeBasicMessage")
    val field_map=messageDescriptor.getFields.asScala
      .groupBy(_.getName.toLowerCase(Locale.ROOT))
      .mapValues(_.toSeq)
    println(field_map)
  }

}
