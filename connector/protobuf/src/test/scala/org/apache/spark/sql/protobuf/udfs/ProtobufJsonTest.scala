package org.apache.spark.sql.protobuf.udfs

import com.google.protobuf.DynamicMessage
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.protobuf.functions.to_protobuf
import org.apache.spark.sql.protobuf.utils.ProtobufUtils
import org.apache.spark.sql.protobuf.{ProtobufSharedSparkSession, ProtobufTestBase}
import  org.apache.spark.sql.protobuf.MockData._

class ProtobufJsonTest extends ProtobufSharedSparkSession with ProtobufTestBase{
  val student1DescFile = protobufDescriptorFile("student1.desc")
  val student2DescFile = protobufDescriptorFile("student2.desc")
  val complexStudentDescFile = protobufDescriptorFile("complex_student.desc")
  private val student1Desc = ProtobufUtils.readDescriptorFileContent(student1DescFile)
  private val student2Desc = ProtobufUtils.readDescriptorFileContent(student2DescFile)
  private val complexStudentDesc = ProtobufUtils.readDescriptorFileContent(complexStudentDescFile)

  test("get_protobuf_json_object"){
     // build a byte
     val studentDF=spark.createDataFrame(studentData,studentSchema)
    //to_proto
    val oldProtoDF=studentDF.withColumn("value",struct("id","name","age","timestamp")
    ).withColumn("proto",to_protobuf(col("value"),"Student",student1Desc)
    ).withColumn("desc_path",lit(student1DescFile)).drop("value")
    val newProtoDF=studentDF.withColumn("value",struct("id","name","age","timestamp","addition_info")
    ).withColumn("proto",to_protobuf(col("value"),"Student",student2Desc)
    ).withColumn("desc_path",lit(student2DescFile)).drop("value")
    val stage1DF=oldProtoDF.unionByName(newProtoDF)
    stage1DF.show(false)
    stage1DF.createOrReplaceTempView("test_table")
    //
    spark.sql("select get_protobuf_json_object(proto,'id','Student',desc_path) from test_table").show(false)
   }


  test("get_protobuf_json_object2"){
    // build a byte
    val studentDF=spark.createDataFrame(complexStudentData,complexStudentSchema)
    //to_proto
    val protoDF=studentDF.withColumn("value",struct("id","name","age","favorite_foods")
    ).withColumn("proto",to_protobuf(col("value"),"ComplexStudent",complexStudentDesc)
    ).withColumn("desc_path",lit(complexStudentDescFile)).drop("value")
    protoDF.show(false)
    protoDF.repartition(1).createOrReplaceTempView("test_table")
    //
    spark.sql("select get_protobuf_json_object(proto,'$.favorite_foods[0]','ComplexStudent',desc_path),get_protobuf_json_object(proto,'$.age','ComplexStudent',desc_path) from test_table").show(false)
    spark.sql("select get_protobuf_json_object(proto,'$.non_exists_field','ComplexStudent',desc_path) from test_table").show(false)
  }

  test("print json object"){
    spark.sql("""select get_json_object('{"a":{"b":"c"},"ar":[{"c":"d"},{"e":"f"}]}','$.ar')""").show()
  }

}
