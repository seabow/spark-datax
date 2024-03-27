package io.github.seabow.datax.core.pipeline.processor

import com.typesafe.config.ConfigFactory
import io.github.seabow.datax.common.{HdfsUtils, HiveUtils}
import io.github.seabow.datax.core.BasePipelineTest
import io.github.seabow.datax.core.mock.MockData
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable.ListBuffer

class MergeSmallFilesProcessorTest  extends BasePipelineTest{
  val processor=new MergeSmallFilesProcessor

  test("merge small files dynamic"){
    //create a merge_small_files_test_table
    val tableName="merge_small_files_test_table"
    val ddl=MockData.mockPartitionTableDDL(tableName,true)
    spark.sql(ddl)
    //insert into
    for(i <- 0 until 10){
      mockDF.write.format("hive").mode("append").insertInto(tableName)
    }
    //now we have a small file table
    val (_,location)=HiveUtils.getPartitionSpecAndLocation(tableName)
    //check
    val fileCount=HdfsUtils.getContentSummary(location).getFileCount
    assert(fileCount==51)
    //default reserve_days is 7 ,the table wouldn't be merged
    val config:String="""
        |{
        | table_col:table_name
        |}
        |""".stripMargin
    processor.config(ConfigFactory.parseString(config))
    val inputDF=spark.sql(s"select '$tableName' as table_name")
    processor.process(ListBuffer(inputDF))
    val fileCountAfterMerge=HdfsUtils.getContentSummary(location).getFileCount
   assert(fileCountAfterMerge==fileCount)

    //use reserve_days 0 ,table will be merge
    val config2:String="""
                        |{
                        | table_col:table_name
                        | reserve_days:0
                        |}
                        |""".stripMargin
    // need to set to dynamic ,otherwise it will  fall back to static
    spark.conf.set(SQLConf.PARTITION_OVERWRITE_MODE.key, "dynamic")
    processor.config(ConfigFactory.parseString(config2))
    processor.process(ListBuffer(inputDF))
    val fileCountAfterMerge2=HdfsUtils.getContentSummary(location).getFileCount
    assert(fileCountAfterMerge2==6)
    dropTable(tableName)
  }

  test("merge small files dynamic fallback to static") {
    //create a merge_small_files_test_table
    val tableName="merge_small_files_test_table"
    val ddl=MockData.mockPartitionTableDDL(tableName,true)
    spark.sql(ddl)
    //insert into
    for(i <- 0 until 10){
      mockDF.write.format("hive").mode("append").insertInto(tableName)
    }
    //now we have a small file table
    val (_,location)=HiveUtils.getPartitionSpecAndLocation(tableName)
    //check
    val fileCount=HdfsUtils.getContentSummary(location).getFileCount
    assert(fileCount==51)

    val inputDF=spark.sql(s"select '$tableName' as table_name")

    //use reserve_days 0 ,table will be merge
    val config:String="""
                         |{
                         | table_col:table_name
                         | reserve_days:0
                         |}
                         |""".stripMargin
    // need to set to dynamic ,otherwise it will  fall back to static
    spark.conf.set(SQLConf.PARTITION_OVERWRITE_MODE.key, "static")
    processor.config(ConfigFactory.parseString(config))
    processor.process(ListBuffer(inputDF))
    val fileCountAfterMerge=HdfsUtils.getContentSummary(location).getFileCount
    assert(fileCountAfterMerge==11)
    dropTable(tableName)
  }

  test("merge small files static") {
    //create a merge_small_files_test_table
    val tableName="merge_small_files_test_table"
    val ddl=MockData.mockPartitionTableDDL(tableName,true)
    spark.sql(ddl)
    //insert into
    for(i <- 0 until 10){
      mockDF.write.format("hive").mode("append").insertInto(tableName)
    }
    //now we have a small file table
    val (_,location)=HiveUtils.getPartitionSpecAndLocation(tableName)
    //check
    val fileCount=HdfsUtils.getContentSummary(location).getFileCount
    assert(fileCount==51)

    val inputDF=spark.sql(s"select '$tableName' as table_name")

    //use reserve_days 0 ,table will be merge
    val config:String="""
                        |{
                        | table_col:table_name
                        | reserve_days:0
                        | dynamic_merge_mode:static
                        |}
                        |""".stripMargin
    // need to set to dynamic ,otherwise it will  fall back to static
    spark.conf.set(SQLConf.PARTITION_OVERWRITE_MODE.key, "dynamic")
    processor.config(ConfigFactory.parseString(config))
    processor.process(ListBuffer(inputDF))
    val fileCountAfterMerge=HdfsUtils.getContentSummary(location).getFileCount
    assert(fileCountAfterMerge==6)
    dropTable(tableName)
  }

}
