package org.apache.spark.sql.protobuf

import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.protobuf.functions._
import org.apache.spark.sql.protobuf.utils.ProtobufUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{QueryTest, Row}

import scala.collection.JavaConverters._

class ProtobufEvalutionTest extends QueryTest with SharedSparkSession with ProtobufTestBase
  with Serializable{
  import testImplicits._

  val student1DescFile = protobufDescriptorFile("student1.desc")
  val student2DescFile = protobufDescriptorFile("student2.desc")
  private val student1Desc = ProtobufUtils.readDescriptorFileContent(student1DescFile)
  private val student2Desc = ProtobufUtils.readDescriptorFileContent(student2DescFile)

  /**
   *    int64 id = 1;
        string name = 2;
        int32 age = 3;
        int64 timestamp = 4;
        string addition_info=5;
   * */
  val studentSchema=StructType(
    List(
      StructField("id",LongType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true),
      StructField("timestamp",LongType,true),
      StructField("addition_info",StringType,true)
    )
  )

  val studentData=List(
    Row(1l,"zhang3",19,1600000000l,"info 1"),
    Row(2l,"li4",20,1600000000l,"info 2"),
    Row(3l,"wang5",21,1600000000l,"info 3"),
    Row(4l,"zhao6",22,1600000000l,"info 4"),
  ).asJava

  test("use old proto in from_protobuf"){
    val studentDF=spark.createDataFrame(studentData,studentSchema)
    //to_proto
    val stage1DF=studentDF.withColumn("value",struct("id","name","age","timestamp","addition_info")
    ).withColumn("proto",to_protobuf(col("value"),"Student",student2Desc))
    stage1DF.show(false)
    //from_proto
    val stage2DF=stage1DF.withColumn("value_after_serde",
      from_protobuf(col("proto"),"Student",student1Desc))
    stage2DF.show(false)
    checkAnswer(stage1DF.select(struct("id","name","age","timestamp"
    ).as("value")),stage2DF.select(col("value_after_serde").as("value")))
  }

  test("use new proto in from_protobuf"){
    val studentDF=spark.createDataFrame(studentData,studentSchema)
    //to_proto
    val stage1DF=studentDF.withColumn("value",struct("id","name","age","timestamp")
    ).withColumn("proto",to_protobuf(col("value"),"Student",student1Desc))
    stage1DF.show(false)
    //from_proto
    val stage2DF=stage1DF.withColumn("value_after_serde",
      from_protobuf(col("proto"),"Student",student2Desc))
    stage2DF.show(false)
    checkAnswer(stage1DF.select(struct($"id",$"name",$"age",$"timestamp",lit(null).as("addition_info")
    ).as("value")),stage2DF.select(col("value_after_serde").as("value")))
  }

  test("evolution protobuf"){
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
    //from_proto
    val stage2DF=stage1DF.withColumn("value_after_serde",
      from_protobuf(col("proto"),col("desc_path"),"Student",studentSchema.toDDL))
    stage2DF.show(false)
//    checkAnswer(stage1DF.select(struct($"id",$"name",$"age",$"timestamp",lit(null).as("addition_info")
//    ).as("value")),stage2DF.select(col("value_after_serde").as("value")))
  }


  /**
   *TODO: if the struct has any value not in the proto? maybe we should drop that value but remain other values
   */
//  test("use old proto in to_protobuf"){
//    val studentDF=spark.createDataFrame(studentData,studentSchema)
//    //to_proto
//    val stage1DF=studentDF.withColumn("value",struct("id","name","age","timestamp","addition_info")
//    ).withColumn("proto",to_protobuf(col("value"),"Student",student1Desc))
//    stage1DF.show(false)
//    //from_proto
//    val stage2DF=stage1DF.withColumn("value_after_serde",
//      from_protobuf(col("proto"),"Student",student2Desc))
//    stage2DF.show(false)
//    checkAnswer(stage1DF.select(struct("id","name","age","timestamp"
//    ).as("value")),stage2DF.select(col("value_after_serde").as("value")))
//  }

}
