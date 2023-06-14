import io.github.seabow.datax.core.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._

class SimpleSmokeTest extends  AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper{

  val userDefinedSchema_01 = StructType(
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
      StructField("ImpDay", IntegerType, true),
    )
  )
  val expectedData_01 = List(
    Row(false,Date.valueOf("2023-05-29"),Timestamp.valueOf("2023-05-29 00:00:01"),
      "CA869", "Phạm Uyển Trinh", null, null, 2200.01f, null, Array("1", "2", "3"),
      Map(),Map("sport"->Array("football", "basketball")), Row("John Doe", 30,Array("football", "basketball")),20230529),
    Row(true,Date.valueOf("2023-05-29"), Timestamp.valueOf("2023-05-29 00:00:02"),
      "CA870", "Nguyễn Liên Thảo", null, null, 2000.02f, 1350.05d, Array("1", "2", "3"),
      null,Map("sport"->Array("football")), Row("Jane Smith", 25,Array("football")),20230529),
    Row(false,Date.valueOf("2023-05-30"),Timestamp.valueOf("2023-05-30 00:00:01") ,
      "CA871", "Lê Thị Nga", 17000, null, null, null, Array("1", "2", "3"),
      Map("color" -> "yellow", "0.3" -> "1"),Map("sport"->Array("football", "basketball", "tennis")), Row("David Johnson", 40,Array("football", "basketball", "tennis")),20230530),
    Row(false,Date.valueOf("2023-05-31"),Timestamp.valueOf("2023-05-31 00:00:01") ,
      "CA872", "Phan Tố Nga", null, null, 2000.02f, null, Array("1", "2", "3"),
      Map("color" -> "blue", "0.3" -> "1"), Map(),Row("Sarah Williams", 35,Array()),20230531),
    Row(false,Date.valueOf("2023-06-01"),Timestamp.valueOf("2023-06-01 00:00:01") ,
      "CA873", "Nguyễn Thị Teresa Teng", null, 132324l, 1200.03f, 1350.06d,
      Array("1", "2", "3","4"), Map("color" -> "red", "0.5" -> "2"),null,
      Row("Michael Brown", 45,null),20230601)
  ).asJava

  val testDF=spark.createDataFrame(expectedData_01,userDefinedSchema_01)
  val hdfs=FileSystem.get(new Configuration)

 override def beforeAll(){
    testDF.write.mode("overwrite").parquet("test_data/prepare_data")
  }

   test("read and write"){
     val confPath="core/src/test/conf/test_read_and_write.conf"
     val confContent=getConfContentFromPath(confPath)
     println(confContent)
     Job(confContent,spark=spark).execute()
     spark.read.parquet("test_data/post_data").show(false)
   }
}
