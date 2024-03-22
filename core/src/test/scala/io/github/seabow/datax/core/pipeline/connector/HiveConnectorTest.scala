package io.github.seabow.datax.core.pipeline.connector

import com.typesafe.config.ConfigFactory
import io.github.seabow.datax.common.HiveUtils
import io.github.seabow.datax.core.BasePipelineTest
import io.github.seabow.datax.core.mock.MockData
import io.github.seabow.datax.core.testutils.TestHiveMetastore
import org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS
import org.apache.iceberg.CatalogUtil
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.exceptions.AlreadyExistsException
import org.apache.iceberg.hive.HiveCatalog

import scala.collection.JavaConverters._

class HiveConnectorTest  extends  BasePipelineTest{
  val hiveConnector= new HiveConnector
  val metastore= new TestHiveMetastore

  override def beforeAll(){
     metastore.start()
     val hiveConf=metastore.hiveConf()
     spark.conf.set("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
    val catalog=CatalogUtil.loadCatalog(classOf[HiveCatalog].getName,"iceberg",Map.empty[String,String].asJava,hiveConf).asInstanceOf[HiveCatalog]
    try catalog.createNamespace(Namespace.of("test_db"))
    catch {
      case ignored: AlreadyExistsException =>
      // the default namespace already exists. ignore the create error
    }
  }

  override def afterAll(): Unit ={
    metastore.stop()
  }
//
  test("overwrite to existing table"){
    //create a test hive table
    spark.sql(
      """
        |create table if not exists test_hive_table(
        | name string,
        | age int
        |) stored as orc
        |""".stripMargin)
    val data = Seq(("Value 1", 10), ("Value 2", 20), ("Value 3", 30),("Value 1", 10), ("Value 2", 20), ("Value 3", 30))

    val df = spark.createDataFrame(data).toDF("name", "age")
    val config=
      """
        |    {
        |      name:"write"
        |      type:"hive"
        |      stage:"writer"
        |      mode:"overwrite"
        |      table:"test_hive_table"
        |    }
        |""".stripMargin
    hiveConnector.config(ConfigFactory.parseString(config))
    val value=hiveConnector.write(df)
    println(s"value is $value")
    dropTable("test_hive_table")
  }

  test("write to non exist hive table:no partition") {
    val tableName="non_exist_table"
    dropTable(tableName)
    val config=
      s"""
        |    {
        |      name:"write"
        |      type:"hive"
        |      stage:"writer"
        |      mode:"overwrite"
        |      table:"$tableName"
        |    }
        |""".stripMargin
    val configObject=ConfigFactory.parseString(config)
    hiveConnector.config(configObject)
    hiveConnector.write(mockDF)
    val actualDF=spark.read.table(tableName)
    assertSmallDatasetEquality(actualDF,mockDF)
    dropTable(tableName)
  }

  test("write to non exist hive table:partition by") {
    val tableName="non_exist_partition_table"
    dropTable(tableName)
    val config=
      s"""
         |    {
         |      name:"write"
         |      type:"hive"
         |      stage:"writer"
         |      mode:"overwrite"
         |      partition_by:[ImpDay,ImpHour]
         |      table:"$tableName"
         |    }
         |""".stripMargin
    val configObject=ConfigFactory.parseString(config)
    hiveConnector.config(configObject)
    hiveConnector.write(mockDF)
    val actualDF=spark.read.table(tableName)
    assertSmallDatasetEquality(actualDF.orderBy("ImpDay","ImpHour"),mockDF.orderBy("ImpDay","ImpHour"))
    val (partitions,_)=HiveUtils.getPartitionSpecAndLocation(tableName)
    assert(partitions.equals("ImpDay,ImpHour"))
    dropTable(tableName)
  }

  test("write to exist hive table:partition spec") {
    val tableName="exist_partition_table"
    dropTable(tableName)
    spark.sql(MockData.mockPartitionTableDDL(tableName))

    val config=
      s"""
         |    {
         |      name:"write"
         |      type:"hive"
         |      stage:"writer"
         |      mode:"overwrite"
         |      partition_spec:"ImpDay='20230529',ImpHour='01'"
         |      table:"$tableName"
         |    }
         |""".stripMargin
    val configObject=ConfigFactory.parseString(config)
    hiveConnector.config(configObject)
    val filteredMockDF=mockDF.filter("ImpDay='20230529' and ImpHour='01'")
    hiveConnector.write(filteredMockDF)
    val actualDF=spark.read.table(tableName)
    assertSmallDatasetEquality(actualDF,filteredMockDF)

    val config2=
      s"""
         |    {
         |      name:"write2"
         |      type:"hive"
         |      stage:"writer"
         |      mode:"append"
         |      partition_spec:"ImpDay='20230529',ImpHour='01'"
         |      table:"$tableName"
         |    }
         |""".stripMargin
    hiveConnector.config(ConfigFactory.parseString(config2))
    hiveConnector.write(filteredMockDF)
    val actualDF2=spark.read.table(tableName)
    assertSmallDatasetEquality(actualDF2,filteredMockDF.unionAll(filteredMockDF))
    dropTable(tableName)
  }

  test("write to exist iceberg table") {
    val tableName="exist_iceberg_partition_table"
    dropTable(tableName)
    spark.sql(MockData.mockIcebergPartitionTableDDL(tableName))
    val config=
      s"""
         |    {
         |      name:"write"
         |      type:"hive"
         |      stage:"writer"
         |      mode:"overwrite"
         |      table:"$tableName"
         |    }
         |""".stripMargin
    val configObject=ConfigFactory.parseString(config)
    hiveConnector.config(configObject)
    val filteredMockDF=mockDF.filter("ImpDay='20230529' and ImpHour='01'")
    hiveConnector.write(filteredMockDF)
    val actualDF=spark.read.table(tableName)
    assertSmallDatasetEquality(actualDF,filteredMockDF)

    val config2=
      s"""
         |    {
         |      name:"write2"
         |      type:"hive"
         |      stage:"writer"
         |      mode:"append"
         |      table:"$tableName"
         |    }
         |""".stripMargin
    hiveConnector.config(ConfigFactory.parseString(config2))
    hiveConnector.write(filteredMockDF)
    val actualDF2=spark.read.table(tableName)
    assertSmallDatasetEquality(actualDF2,filteredMockDF.unionAll(filteredMockDF))
    dropTable(tableName)
  }

  test("write to exist iceberg table with sparkCatalog") {
    val tableName="iceberg.test_db.exist_iceberg_partition_table"
    dropTable(tableName)
    spark.sql(MockData.mockIcebergPartitionTableDDL(tableName))
    val config=
      s"""
         |    {
         |      name:"write"
         |      type:"hive"
         |      stage:"writer"
         |      mode:"overwrite"
         |      table:"$tableName"
         |    }
         |""".stripMargin
    val configObject=ConfigFactory.parseString(config)
    hiveConnector.config(configObject)
    val filteredMockDF=mockDF.filter("ImpDay='20230529' and ImpHour='01'")
    hiveConnector.write(filteredMockDF)
    val actualDF=spark.read.table(tableName)
    assertSmallDatasetEquality(actualDF,filteredMockDF)

    val config2=
      s"""
         |    {
         |      name:"write2"
         |      type:"hive"
         |      stage:"writer"
         |      mode:"append"
         |      table:"$tableName"
         |    }
         |""".stripMargin
    hiveConnector.config(ConfigFactory.parseString(config2))
    hiveConnector.write(filteredMockDF)
    val actualDF2=spark.read.table(tableName)
    assertSmallDatasetEquality(actualDF2,filteredMockDF.unionAll(filteredMockDF))
    dropTable(tableName)
  }

  test("default dynamic overwrite table") {
    val tableName="dynamic_overwrite_hive_table"
    val icebergTableName="dynamic_overwrite_iceberg_table"
    dropTable(tableName)
    dropTable(icebergTableName)
    spark.sql(MockData.mockPartitionTableDDL(tableName))
    spark.sql(MockData.mockIcebergPartitionTableDDL(icebergTableName))

    def overwriteConfig(table:String)= {
      val config=s"""
         |    {
         |      name:"write"
         |      type:"hive"
         |      stage:"writer"
         |      mode:"overwrite"
         |      table:"$table"
         |    }
         |""".stripMargin
     ConfigFactory.parseString(config)
    }
    hiveConnector.config(overwriteConfig(tableName))
    hiveConnector.write(mockDF)
    val actualDF1=spark.read.table(tableName)
    assertSmallDatasetEquality(actualDF1.orderBy("ImpDay","ImpHour"),
      mockDF.orderBy("ImpDay","ImpHour"))
    val filteredMockDF=mockDF.filter("ImpDay='20230529' and ImpHour='01'")
    hiveConnector.write(filteredMockDF)
    val actualDF2=spark.read.table(tableName)
    assertSmallDatasetEquality(actualDF2.orderBy("ImpDay","ImpHour"),
      mockDF.orderBy("ImpDay","ImpHour"))


    hiveConnector.config(overwriteConfig(icebergTableName))
    hiveConnector.write(mockDF)
    val actualDF3=spark.read.table(icebergTableName)
    assertSmallDatasetEquality(actualDF3.orderBy("ImpDay","ImpHour"),
      mockDF.orderBy("ImpDay","ImpHour"))
    hiveConnector.write(filteredMockDF)
    val actualDF4=spark.read.table(icebergTableName)
    assertSmallDatasetEquality(actualDF4.orderBy("ImpDay","ImpHour"),
      mockDF.orderBy("ImpDay","ImpHour"))
    dropTable(tableName)
    dropTable(icebergTableName)
  }


  def dropTable(tableName:String):Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }
}
