package io.github.seabow.datax.core.pipeline.processor

import com.typesafe.config.ConfigFactory
import io.github.seabow.datax.common.HiveUtils
import io.github.seabow.datax.core.BasePipelineTest
import io.github.seabow.datax.core.mock.MockData

import scala.collection.mutable.ListBuffer

class FsContentSummaryProcessorTest  extends BasePipelineTest{
  val processor=new FsContentSummaryProcessor

  test("get fs content summary of a table") {
    val tableName="test_fs_content_summary_table"
    val createDDL=MockData.mockPartitionTableDDL(tableName)
    spark.sql(createDDL)
    MockData.mockDF.write.mode("overwrite").format("hive").insertInto(tableName)
    val (_,location)=HiveUtils.getPartitionSpecAndLocation(tableName)
    val inputDF=spark.sql(s"select '$location' as path")
    val config=
      """
        |{
        | name:test_fs_content_summary_table
        |
        |}
        |""".stripMargin
        processor.config(ConfigFactory.parseString(config))
    val processedDF=processor.process(ListBuffer(inputDF))
    processedDF.show(false)
    val contentSummaryRow=processedDF.collect().head
    val content_summary_dir_cnt=contentSummaryRow.getAs[Long]("content_summary_dir_cnt")
    val content_summary_file_cnt=contentSummaryRow.getAs[Long]("content_summary_file_cnt")
    val content_summary_length=contentSummaryRow.getAs[Long]("content_summary_length")
    assert(content_summary_dir_cnt==10)
    assert(content_summary_file_cnt>=5)
    assert(content_summary_length==12011)
    dropTable(tableName)
  }

}
