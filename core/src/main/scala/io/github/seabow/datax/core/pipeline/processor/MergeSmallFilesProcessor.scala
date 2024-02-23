package io.github.seabow.datax.core.pipeline.processor

import io.github.seabow.datax.common.ConfigUtils._
import io.github.seabow.datax.common.HdfsUtils
import io.github.seabow.datax.core.pipeline.Processor
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

object MergeSmallFilesProcessorConfig {
  val table_col = "table_col"
  val reserve_days="reserve_days"
  val target_file_size_mb="target_file_size_mb"
  val max_dirs_per_group="max_dirs_per_group"
  val max_gb_per_group="max_gb_per_group"
}

//两种合并模式:
//1.大分区文件合并，单个分区中的文件总大小大于等于256MB。
//2.小分区文件合并，单个分区中的文件总大小小于256MB。
//挑选需要合并的分区。
//读取分区中的数据。
//insert overwrite 回原表。
class MergeSmallFilesProcessor extends Processor with Logging {
  override def process(dfList: ListBuffer[DataFrame]): DataFrame = {
    //遍历一级分区
    val table_col=config.getString(MergeSmallFilesProcessorConfig.table_col,"table_name")
    val reserve_days=config.getInt(MergeSmallFilesProcessorConfig.reserve_days,7)
    val target_file_size_mb=config.getInt(MergeSmallFilesProcessorConfig.target_file_size_mb,128)
    val max_dirs_per_group=config.getInt(MergeSmallFilesProcessorConfig.target_file_size_mb,1000)
    val max_gb_per_group=config.getInt(MergeSmallFilesProcessorConfig.max_gb_per_group,100)
    spark.sqlContext.setConf("spark.sql.adaptive.advisoryPartitionSizeInBytes",(target_file_size_mb*1024*10224).toString)
    spark.sqlContext.setConf("spark.sql.adaptive.enabled","true")
    val inputDF = dfList(0)
    def getPartitionSpecAndLocation(table:String):(String,String)={
      val createStatDF=spark.sql(s"show create table $table")
      val createDDL=createStatDF.collect().head.getString(0).replaceAll("\n"," ")
      val partten=""".*PARTITIONED BY \((.*)\) LOCATION '(.*)' TBLPROPERTIES .*""".r
      val partten(partitionSpec,location)=createDDL
      (partitionSpec,location)
    }
    def getPartitionStats(partitionDirs: Seq[FileStatus]): Array[Row] = {
      import spark.implicits._
      val dirsDF = partitionDirs.map(status=>Seq(status.getPath.toString,status.getModificationTime)).toDF("path","modify_time")
      val newSchema = dirsDF.schema.add("content_summary_length", LongType
      ).add("content_summary_dir_cnt", LongType
      ).add("content_summary_file_cnt", LongType
      ).add("content_summary_space_consumed", LongType)
      dirsDF.mapPartitions {
        partition =>
          val opResultFutures = partition.map {
            row =>
              val path = row.getAs[String]("path")
              Future {
                try {
                  val contentSummary = HdfsUtils.getContentSummary(path)
                  Some(Row.fromSeq(row.toSeq ++ Seq[Any](contentSummary.getLength
                    , contentSummary.getDirectoryCount, contentSummary.getFileCount, contentSummary.getSpaceConsumed)))
                } catch {
                  case e: Exception => None
                }
              }
          }
          Await.result(Future.sequence(opResultFutures), scala.concurrent.duration.Duration.Inf).filter(
            _.isDefined
          ).map(_.get)
      }(RowEncoder(newSchema)).collect()
    }
    inputDF.collect().foreach{
      r=>
        val table=r.getAs[String](table_col)
        log.warn(s"start merge table $table")
        val (partitionSpec,location)=getPartitionSpecAndLocation(table)
        // 遍历一级分区，对待合并分区进行分组，输出partitionGroups
        //顺序:old first; 分区数量:100, 限制:排除modifytime xx天以内的分区，大小:最大单次合并100G。
        val partitionDirs=HdfsUtils.listDirs(location).filter(_.getPath.toString.contains("=")
        ).filter(System.currentTimeMillis()-_.getModificationTime>reserve_days*24*60*60*1000l).toSeq
        val partitionStats=getPartitionStats(partitionDirs).sortBy(_.getAs[Long]("modify_time"))
        var partitionGroups=ListBuffer.empty[ListBuffer[Row]]
        var currentPartitionGroup=ListBuffer.empty[Row]
        var currentGroupLength=0l
        var currentGroupDirs=0l
        partitionStats.foreach{
          partition=>
            val fileLength = partition.getAs[Long]("content_summary_length")
            val fileCount = partition.getAs[Long]("content_summary_file_cnt")
            val dirCount = partition.getAs[Long]("content_summary_dir_cnt")
            //如果分区需要合并(平均文件大小<target_file_size_mb*75% 且 fileCount/dirsCount+1>=5)，则继续
            val needToMerge=(fileLength/ (fileCount+1)<target_file_size_mb*1024*1024*0.75) &&(fileCount/(dirCount+1)>=5)
            if(needToMerge){
              //计算当前partitionGroup是否已经满了
              val isGroupFull= (currentGroupDirs+dirCount>max_dirs_per_group) ||(currentGroupLength+fileLength) > max_gb_per_group *1024*1024*1024l
              if (isGroupFull){
                partitionGroups.append(currentPartitionGroup)
                currentPartitionGroup=ListBuffer.empty[Row]
                currentGroupLength=0l
                currentGroupDirs=0l
              }
                currentPartitionGroup.append(partition)
                currentGroupLength=currentGroupLength+fileLength
                currentGroupDirs=currentGroupDirs+dirCount
            }
        }
        if(currentPartitionGroup.nonEmpty){
          partitionGroups.append(currentPartitionGroup)
        }

        //依次处理partitionGroups的合并
        val partitionValuePattern=".*=(.*)".r
        partitionGroups.foreach{
          partitionGroup=>
            val topPartitions=partitionGroup.map{r=>
              val path=r.getAs[String]("path")
              val partitionValuePattern(partitionValue)=path
              s"'$partitionValue'"
            }.mkString(",")
          val topPartitionCol=partitionSpec.split(",").head
          val mergeSql=  s"""insert overwrite $table
              |select * from $table
              |where $topPartitionCol in ($topPartitions)
              |distributed by $partitionSpec""".stripMargin
            log.warn(s"$mergeSql")
           spark.sql(mergeSql)
        }
    }
    spark.emptyDataFrame
  }

  override def shortName(): String = "merge_small_files"
}

