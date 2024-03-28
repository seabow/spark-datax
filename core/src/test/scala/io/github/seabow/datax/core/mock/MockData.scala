package io.github.seabow.datax.core.mock

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._

object MockData {
 private val userDefinedSchema_01 = StructType(
    List(
      StructField("isDeleted",BooleanType,true),
      StructField("Day", DateType, true),
      StructField("Timestamp", TimestampType, true),
      StructField("CustomerID", StringType, true),
      StructField("CustomerName", StringType, true),
      StructField("StandardPackage", IntegerType, true),
      StructField("ExtraOption1", LongType, true),
      StructField("ExtraOption2", FloatType, true),
      StructField("ExtraOption3", DoubleType, true),
      StructField("ArrayInfo", ArrayType(StringType), true),
      StructField("MapInfo", MapType(StringType, StringType), true),
      StructField("MapArray", MapType(StringType, ArrayType(StringType)), true),
      StructField("StructInfo", StructType(Seq(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("map_tags",ArrayType(StringType),true)
      )), true),
      StructField("ImpDay", StringType, true),
      StructField("ImpHour", StringType, true),
    )
  )

  private val expectedData_01 = List(
    Row(false,Date.valueOf("2023-05-29"),Timestamp.valueOf("2023-05-29 00:00:01"),
      "CA869", "Phạm Uyển Trinh", null, null, 2200.01f, null, Array("1", "2", "3"),
      Map(),Map("sport"->Array("football", "basketball")), Row("John Doe", 30,Array("football", "basketball")),"20230529","01"),
    Row(true,Date.valueOf("2023-05-29"), Timestamp.valueOf("2023-05-29 00:00:02"),
      "CA870", "Nguyễn Liên Thảo", null, null, 2000.02f, 1350.05d, Array("1", "2", "3"),
      null,Map("sport"->Array("football")), Row("Jane Smith", 25,Array("football")),"20230529","02"),
    Row(false,Date.valueOf("2023-05-30"),Timestamp.valueOf("2023-05-30 00:00:01") ,
      "CA871", "Lê Thị Nga", 17000, null, null, null, Array("1", "2", "3"),
      Map("color" -> "yellow", "0.3" -> "1"),Map("sport"->Array("football", "basketball", "tennis")), Row("David Johnson", 40,Array("football", "basketball", "tennis")),"20230530","11"),
    Row(false,Date.valueOf("2023-05-31"),Timestamp.valueOf("2023-05-31 00:00:01") ,
      "CA872", "Phan Tố Nga", null, null, 2000.02f, null, Array("1", "2", "3"),
      Map("color" -> "blue", "0.3" -> "1"), Map(),Row("Sarah Williams", 35,Array()),"20230531","12"),
    Row(false,Date.valueOf("2023-06-01"),Timestamp.valueOf("2023-06-01 00:00:01") ,
      "CA873", "Nguyễn Thị Teresa Teng", null, 132324l, 1200.03f, 1350.06d,
      Array("1", "2", "3","4"), Map("color" -> "red", "0.5" -> "2"),null,
      Row("Michael Brown", 45,null),"20230601","21")
  ).asJava

  lazy val mockDF=SparkSession.active.createDataFrame(expectedData_01,userDefinedSchema_01)

  def mockTableDDL(tableName:String):String={
    s"create table if not exists $tableName (${userDefinedSchema_01.toDDL}) using orc"
  }

  def mockPartitionTableDDL(tableName:String,useLowerCase:Boolean=false,useHiveFormat:Boolean=false):String={
    var ddl=userDefinedSchema_01.fields.dropRight(2).map(_.toDDL).mkString(",")
    var partitionSpec="(ImpDay string,ImpHour string)"
    if(useLowerCase){
      ddl=ddl.toLowerCase
      partitionSpec=partitionSpec.toLowerCase
    }
    val sparkOrHiveFormat= if(useHiveFormat) "stored as" else "using"
    s"create table if not exists $tableName ($ddl) $sparkOrHiveFormat orc partitioned by $partitionSpec"
  }

  def mockIcebergPartitionTableDDL(tableName:String):String={
    s"create table if not exists $tableName (${userDefinedSchema_01.toDDL}) using iceberg partitioned by (ImpDay,ImpHour)"
  }
}
