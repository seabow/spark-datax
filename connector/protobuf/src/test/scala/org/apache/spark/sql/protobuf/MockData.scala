package org.apache.spark.sql.protobuf

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}

import scala.collection.JavaConverters._

object MockData {

  val studentData=List(
    Row(1l,"zhang3",19,1600000000l,"info 1"),
    Row(2l,"li4",20,1600000000l,"info 2"),
    Row(3l,"wang5",21,1600000000l,"info 3"),
    Row(4l,"zhao6",22,1600000000l,"info 4"),
  ).asJava

  val complexStudentData=List(
    Row(1l,"zhang3",19,Array("egg")),
    Row(2l,"li4",20,Array("egg")),
    Row(3l,"wang5",21,Array("egg")),
    Row(4l,"zhao6",22,Array("egg")),
  ).asJava

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
  val complexStudentSchema=StructType(
    List(
      StructField("id",LongType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true),
      StructField("favorite_foods",ArrayType(StringType),true)
    )
  )
}
