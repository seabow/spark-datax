package io.github.seabow.datax.connector.jdbc

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.github.seabow.datax.common.ConfigUtils
import io.github.seabow.datax.connector.JdbcConnector
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp
import scala.collection.JavaConverters._
class JdbcConnectorTest extends AnyFunSuite with SparkSessionTestWrapper with BeforeAndAfterAll with DatasetComparer  {

  val userDefinedSchema_01 = StructType(
    List(
      StructField("isDeleted",BooleanType,true),
      StructField("Timestamp", TimestampType, true),
      StructField("CustomerID", StringType, true),
      StructField("CustomerName", StringType, true),
      StructField("StandardPackage", IntegerType, true),
      StructField("ExtraOption1", LongType, true),
      StructField("ExtraOption2", DoubleType, true),
      StructField("ExtraOption3", DoubleType, true),
      StructField("ImpDay", IntegerType, true),
    )
  )
  val expectedData_01 = List(
    Row(false,Timestamp.valueOf("2023-05-29 00:00:01"),
      "CA869", "David", null, null, 2200.01d, null,20230529),
    Row(true, Timestamp.valueOf("2023-05-29 00:00:02"),
      "CA870", "Jim", null, null, 2000.02d, 1350.05d, 20230529),
    Row(false,Timestamp.valueOf("2023-05-30 00:00:01") ,
      "CA871", "Alex", 17000, null, null, null, 20230530),
    Row(false,Timestamp.valueOf("2023-05-31 00:00:01") ,
      "CA872", "Lucy", null, null, 2000.02d, null,20230531),
    Row(false,Timestamp.valueOf("2023-06-01 00:00:01") ,
      "CA873", "Lily", null, 132324l, 1200.03d, 1350.06d, 20230601)
  ).asJava

  val testDF=spark.createDataFrame(expectedData_01,userDefinedSchema_01)

  import ch.vorburger.mariadb4j.DB

  var db: DB = _
  override def beforeAll(): Unit = {

    db = DB.newEmbeddedDB(3306)
    db.start()
  }
  override def afterAll(): Unit = {
    db.stop()
  }

  test("connect jdbc and write"){
      val configContent=
        s"""
          |    {
          |        schema:"${userDefinedSchema_01.toDDL}"
          |        options:{
          |        url:"jdbc:mysql://localhost:3306/test"
          |        dbtable:"test_table"
          |        user:"root"
          |        mode:"overwrite"
          |        password:""
          |        }
          |    }
          |""".stripMargin
          println(configContent)
           val config=ConfigUtils.parseAndResolveContent(configContent)
       val jdbcConnector=new JdbcConnector
       jdbcConnector.config(config)
       jdbcConnector.write(testDF)
       val actualDF=jdbcConnector.read()
       assertSmallDatasetEquality(actualDF, testDF)

  }
}
