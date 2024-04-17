/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.protobuf

import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.DynamicMessage
import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters}
import org.apache.spark.sql.protobuf.utils.ProtobufUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Tests for [[ProtobufSerializer]] and [[ProtobufDeserializer]] with a more specific focus on
 * those classes.
 */
class ProtobufSerdeSuite extends SharedSparkSession with ProtobufTestBase {

  import ProtoSerdeSuite.MatchType._
  import ProtoSerdeSuite._

  private val testFileDescFile = protobufDescriptorFile("serde_suite.desc")
  private val testFileDesc = ProtobufUtils.readDescriptorFileContent(testFileDescFile)

  private val javaClassNamePrefix = "org.apache.spark.sql.protobuf.protos.SerdeSuiteProtos$"

  private val proto2DescFile = protobufDescriptorFile("proto2_messages.desc")
  private val proto2Desc = ProtobufUtils.readDescriptorFileContent(proto2DescFile)

  test("Test basic conversion") {
    withFieldMatchType { fieldMatch =>
      val protoFile = ProtobufUtils.buildDescriptor(testFileDesc, "SerdeBasicMessage")

      val dynamicMessageFoo = DynamicMessage
        .newBuilder(protoFile.getFile.findMessageTypeByName("Foo"))
        .setField(protoFile.getFile.findMessageTypeByName("Foo").findFieldByName("bar"), 10902)
        .build()

      val dynamicMessage = DynamicMessage
        .newBuilder(protoFile)
        .setField(protoFile.findFieldByName("foo"), dynamicMessageFoo)
        .build()

      val serializer = Serializer.create(CATALYST_STRUCT, protoFile, fieldMatch)
      val deserializer = Deserializer.create(CATALYST_STRUCT, protoFile, fieldMatch)

      assert(
        serializer.serialize(deserializer.deserialize(dynamicMessage).get) === dynamicMessage)
    }
  }

  test("Optional fields can be dropped from input SQL schema for the serializer") {
    // This test verifies that optional fields can be missing from input Catalyst schema
    // while serializing rows to protobuf.

    val desc = ProtobufUtils.buildDescriptor(proto2Desc, "FoobarWithRequiredFieldBar")

    // Confirm desc contains optional field 'foo' and required field bar.
    assert(desc.getFields.size() == 2)
    assert(desc.findFieldByName("foo").isOptional)

    // Use catalyst type without optional "foo".
    val sqlType = structFromDDL("struct<bar int>")
    val serializer = new ProtobufSerializer(sqlType, desc, nullable = false) // Should work fine.

    // Should be able to deserialize a row.
    val protoMessage = serializer.serialize(InternalRow(22)).asInstanceOf[DynamicMessage]

    // Verify the descriptor and the value.
    assert(protoMessage.getDescriptorForType == desc)
    assert(protoMessage.getField(desc.findFieldByName("bar")).asInstanceOf[Int] == 22)
  }


  def withFieldMatchType(f: MatchType => Unit): Unit = {
    MatchType.values.foreach { fieldMatchType =>
      withClue(s"fieldMatchType == $fieldMatchType") {
        f(fieldMatchType)
      }
    }
  }
}

object ProtoSerdeSuite {

  val CATALYST_STRUCT =
    new StructType().add("foo", new StructType().add("bar", IntegerType))

  /**
   * Specifier for type of field matching to be used for easy creation of tests that do by-name
   * field matching.
   */
  object MatchType extends Enumeration {
    type MatchType = Value
    val BY_NAME = Value
  }

  import MatchType._

  /**
   * Specifier for type of serde to be used for easy creation of tests that do both serialization
   * and deserialization.
   */
  sealed trait SerdeFactory[T] {
    def create(sqlSchema: StructType, descriptor: Descriptor, fieldMatchType: MatchType): T
  }

  object Serializer extends SerdeFactory[ProtobufSerializer] {
    override def create(
        sql: StructType,
        descriptor: Descriptor,
        matchType: MatchType): ProtobufSerializer = new ProtobufSerializer(sql, descriptor, false)
  }

  object Deserializer extends SerdeFactory[ProtobufDeserializer] {
    override def create(
        sql: StructType,
        descriptor: Descriptor,
        matchType: MatchType): ProtobufDeserializer =
      new ProtobufDeserializer(descriptor, sql, new NoopFilters)
  }
}
