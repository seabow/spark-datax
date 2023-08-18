package org.apache.spark.sql.protobuf.utils

import org.scalatest.funsuite.AnyFunSuite

class ProtobufUtilsTest extends AnyFunSuite {

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
  }

}
