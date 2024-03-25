package io.github.seabow.datax.core.pipeline.processor

import com.typesafe.config.ConfigFactory
import io.github.seabow.datax.common.{HdfsUtils, HiveUtils}
import io.github.seabow.datax.core.BasePipelineTest
import io.github.seabow.datax.core.mock.MockData
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable.ListBuffer

class TempDirCleanProcessorTest  extends BasePipelineTest{
  val processor=new TempDirCleanProcessor
  val tempSuffix="/_temporary/0/_temporary/"

  test("clean tmp dir of a table") {
    val tableName="test_clean_tmp_dir_table"
    val createDDL=MockData.mockPartitionTableDDL(tableName)
    spark.sql(createDDL)
    //need to set hadoop fileoutputcommitter  to v2 and cleanup skipped
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version","2")
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.cleanup.skipped","true")
    spark.conf.set(SQLConf.PARTITION_OVERWRITE_MODE.key, "dynamic")
    MockData.mockDF.write.mode("overwrite").format("hive").insertInto(tableName)
    spark.conf.set(SQLConf.PARTITION_OVERWRITE_MODE.key, "static")
    MockData.mockDF.write.mode("overwrite").format("hive").insertInto(tableName)
    val (_,location)=HiveUtils.getPartitionSpecAndLocation(tableName)
    val inputDF=spark.sql(s"select '$location' as path")
    def configReserveHour(hour:Int)=
      s"""
        |{
        | reserve_hours:$hour
        |}
        |""".stripMargin
    processor.config(ConfigFactory.parseString(configReserveHour(1)))
    assert(HdfsUtils.listDirs(location+tempSuffix).size==1)
    assert(HdfsUtils.listDirs(location).filter(_.getPath.toString().contains(".spark-staging-")).size==0)
    processor.process(ListBuffer(inputDF))
    assert(HdfsUtils.listDirs(location+tempSuffix).size==1)
    assert(HdfsUtils.listDirs(location).filter(_.getPath.toString().contains(".spark-staging-")).size==0)
    processor.config(ConfigFactory.parseString(configReserveHour(0)))
    processor.process(ListBuffer(inputDF))
    assert(HdfsUtils.listDirs(location+tempSuffix).size==0)
    assert(HdfsUtils.listDirs(location).filter(_.getPath.toString().contains(".spark-staging-")).size==0)
    dropTable(tableName)
  }

}
