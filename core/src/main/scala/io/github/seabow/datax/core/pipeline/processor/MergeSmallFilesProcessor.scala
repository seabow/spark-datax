package io.github.seabow.datax.core.pipeline.processor

import io.github.seabow.datax.common.ConfigUtils._
import io.github.seabow.datax.common.HdfsUtils
import io.github.seabow.datax.core.pipeline.Processor
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.utils.SparkCatalogUtils
import org.apache.spark.sql.{DataFrame, Row}

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

object MergeSmallFilesProcessorConfig {
  val table_col = "table_col"
  val reserve_days="reserve_days"
  val target_file_size_mb="target_file_size_mb"
  val max_dirs_per_group="max_dirs_per_group"
  val max_gb_per_group="max_gb_per_group"
  val concurrent_merge_group_size="concurrent_merge_group_size"
  val concurrent_static_merge_size="concurrent_static_merge_size"
  val dynamic_merge_mode="dynamic_merge_mode"
}

//两种合并模式:
//1.大分区文件合并，单个分区中的文件总大小大于等于256MB。
//2.小分区文件合并，单个分区中的文件总大小小于256MB。
//挑选需要合并的分区。
//读取分区中的数据。
//insert overwrite 回原表。
class MergeSmallFilesProcessor extends Processor with Logging {

  def getPartitionStats(partitionDirs: Seq[FileStatus]): Array[Row] = {
    if(partitionDirs.isEmpty){
      return Array.empty[Row]
    }
    spark.sparkContext.setJobDescription(s"Get partition stats for ${partitionDirs.size} dirs,${partitionDirs.head.getPath.toString}...${partitionDirs.last.getPath.toString}")
    val dirsDF = spark.createDataFrame(partitionDirs.map(status=>Row.fromSeq(Seq(status.getPath.toString,status.getModificationTime))).toList.asJava,
      StructType(
        List(
          StructField("path",StringType,true),
          StructField("modify_time", LongType, true)))
    ).repartition(partitionDirs.size/10+1)
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

  def dynamicPartitionMerge(table:String,reserve_days:Int,
                            target_file_size_mb:Int,
                            max_dirs_per_group:Int,
                            max_gb_per_group:Int,
                            concurrent_merge_group_size:Int,concurrent_static_merge_size:Int):Unit= {
    val tableMetadata=SparkCatalogUtils.getTableMetadata(table)
    var dynamic_mode="dynamic"
    if(tableMetadata.partitionColumnNames.size>1){
      val partitionCount=spark.sql(s"show partitions $table").count()
      if(partitionCount>30000){
        dynamic_mode="partial_dynamic"
      }
    }
    val groupExecutor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(concurrent_merge_group_size))
    val staticExecutor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(concurrent_static_merge_size))
    // 遍历一级分区，对待合并分区进行分组，输出partitionGroups
    //顺序:old first; 分区数量:100, 限制:排除modifytime xx天以内的分区，大小:最大单次合并100G。
    val partitionDirs=HdfsUtils.listDirs(new Path(tableMetadata.location).toString).filter(_.getPath.toString.contains("=")
    ).filter(System.currentTimeMillis()-_.getModificationTime>reserve_days*24*60*60*1000l).toSeq
    val partitionStats=getPartitionStats(partitionDirs).sortBy(_.getAs[Long]("modify_time"))
    def needToMerge(partition:Row):Boolean = {
      val fileLength = partition.getAs[Long]("content_summary_length")
      val fileCount = partition.getAs[Long]("content_summary_file_cnt")
      val dirCount = partition.getAs[Long]("content_summary_dir_cnt")
      (fileLength/ (fileCount+1)<target_file_size_mb*1024*1024*0.75) &&(fileCount/dirCount>=5)
    }
    def splitToPartitionGroups(partitionStats:Array[Row]):ListBuffer[ListBuffer[Row]]={
      var partitionGroups=ListBuffer.empty[ListBuffer[Row]]
      var currentPartitionGroup=ListBuffer.empty[Row]
      var currentGroupLength=0l
      var currentGroupDirs=0l
      partitionStats.foreach{
        partition=>
          val fileLength = partition.getAs[Long]("content_summary_length")
          val fileCount = partition.getAs[Long]("content_summary_file_cnt")
          val dirCount = partition.getAs[Long]("content_summary_dir_cnt")
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
      if(currentPartitionGroup.nonEmpty){
        partitionGroups.append(currentPartitionGroup)
      }
      partitionGroups
    }
    val partitionStatsToMerge=partitionStats.filter(needToMerge)
    val mergeTasks:ListBuffer[Future[Any]]=ListBuffer.empty
    val partitionGroupIndex=new AtomicInteger(1)
    if(dynamic_mode.equals("dynamic")){
     val partitionGroups= splitToPartitionGroups(partitionStatsToMerge)
      //依次处理partitionGroups的合并
      val partitionValuePattern=".*=(.*)".r
       partitionGroups.foreach{
        partitionGroup=>
          val topPartitions=partitionGroup.map{r=>
            val path=r.getAs[String]("path")
            val partitionValuePattern(partitionValue)=path
            s"'$partitionValue'"
          }.mkString(",")
          val topPartitionCol=tableMetadata.partitionColumnNames.head
          val mergeSql=  s"""insert overwrite $table
                            |select * from $table
                            |where $topPartitionCol in ($topPartitions)
                            |distribute by ${tableMetadata.partitionColumnNames.mkString(",")}""".stripMargin
          val mergeTask=Future{
            val currentIndex = partitionGroupIndex.getAndIncrement()
            spark.sparkContext.setJobGroup(s"dynamic merge $table partition group $currentIndex/${partitionGroups.size}",s"merge partition group $currentIndex/${partitionGroups.size} $topPartitionCol in ($topPartitions)")
            try{
              executeMergeSql(mergeSql)
            }catch {
              case e=>
                partitionGroup.foreach{
                  r=>
                    val partitionPath=r.getAs[String]("path")
                    val partitionValues=partitionPath.split("/").filter(_.contains("="))
                    val partitionSpec=partitionValues.map(_.split("=")).map(array=> s"${array.head}='${array.last}'").head
                    val showPartitionsSql=s"show partitions $table partition ($partitionSpec)"
                    val partitions=spark.sql(showPartitionsSql).collect().map(_.getString(0))
                    staticPartitionsMergeInternal(table,partitions,reserve_days,target_file_size_mb,"dir",staticExecutor)
                }
            }
          }(groupExecutor)
          mergeTasks.append(mergeTask)
      }
      Await.result(Future.sequence(mergeTasks), scala.concurrent.duration.Duration.Inf)
    }else if(dynamic_mode.equals("partial_dynamic")){
    partitionStatsToMerge.foreach{
        partition=>
          val path=partition.getAs[String]("path")
         val staticPart= path.split("/").filter(_.contains("=")).map(_.split("=")).map(array=> s"${array.head}='${array.last}'").head
         val dynamicPart= tableMetadata.partitionColumnNames.tail.mkString(",")
         val mergeSql=s"""insert overwrite $table partition($staticPart,$dynamicPart)
              | select * from ${tableMetadata.provider.get}.`$path`
              | distribute by $dynamicPart
              |""".stripMargin
          mergeTasks.append(Future{
            log.warn(s"$mergeSql")
            try {
              spark.sql(mergeSql)
            }catch {
              case e=>
                val showPartitionsSql=s"show partitions $table partition ($staticPart)"
                val partitions=spark.sql(showPartitionsSql).collect().map(_.getString(0))
                staticPartitionsMergeInternal(table,partitions,reserve_days,target_file_size_mb,"dir",staticExecutor)
            }
          }(groupExecutor))
      }
      Await.result(Future.sequence(mergeTasks), scala.concurrent.duration.Duration.Inf)
    }
  }
  def staticPartitionsMergeInternal(table:String, partitions:Array[String], reserve_days:Int,
                                    target_file_size_mb:Int,
                                    mode:String,
                                    executor:ExecutionContextExecutor):Unit={
    spark.sparkContext.setJobGroup(s"static merge $table partitions",s"merge ${partitions.size} partitions:${partitions.mkString(",")}")
    val tableMetadata=SparkCatalogUtils.getTableMetadata(table)
    val tableDirString=new Path(tableMetadata.location).toString.stripSuffix("/")+"/"
    val partitionDirs=partitions.map(p=>tableDirString+p).filter(HdfsUtils.exist).map(HdfsUtils.getStatus
    ).filter(System.currentTimeMillis()-_.getModificationTime>reserve_days*24*60*60*1000l).toSeq
    val partitionStats=getPartitionStats(partitionDirs).sortBy(_.getAs[Long]("modify_time"))
    val mergeTasks=ListBuffer.empty[Future[Any]]
    val partitionIndex=new AtomicInteger(1)
    def needToMerge(partition:Row):Boolean = {
      val fileLength = partition.getAs[Long]("content_summary_length")
      val fileCount = partition.getAs[Long]("content_summary_file_cnt")
      val dirCount = partition.getAs[Long]("content_summary_dir_cnt")
      (fileLength/ (fileCount+1)<target_file_size_mb*1024*1024*0.75) &&(fileCount/dirCount>=5)
    }
    val partitionStatsNeedToMerge=partitionStats.filter(needToMerge)
    partitionStatsNeedToMerge.foreach{
      partition=>
        val partitionPath=partition.getAs[String]("path")
        val fileLength = partition.getAs[Long]("content_summary_length")
        val fileCount = partition.getAs[Long]("content_summary_file_cnt")
        val dirCount = partition.getAs[Long]("content_summary_dir_cnt")
        //如果分区需要合并(平均文件大小<target_file_size_mb*75% 且 fileCount/dirsCount+1>=5)，则继续

          //tempView命名规则，table_name_partition_values
          val targetFileCount=fileLength/(target_file_size_mb*1024*1024) + 1
          val mergeTask=Future {
            spark.sparkContext.setJobGroup(s"static merge $table partitions",s"merge (${partitionIndex.getAndIncrement()}/${partitionStatsNeedToMerge.size}) $partitionPath: $fileCount=>$targetFileCount,mode:$mode")
            mergeLeafPartition(table, partitionPath, targetFileCount.toInt,mode,tableMetadata.provider.get)
          }(executor)
          mergeTasks.append(mergeTask)
    }
    Await.result(Future.sequence(mergeTasks), scala.concurrent.duration.Duration.Inf)
  }

  def staticPartitionMerge(table:String,reserve_days:Int,
                                  target_file_size_mb:Int,
                           concurrent_static_merge_size:Int):Unit={
    spark.sparkContext.setJobDescription(s"show partitions $table")
    val partitions=spark.sql(s"show partitions $table").collect().map(_.getString(0))
    val executor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(concurrent_static_merge_size))
    staticPartitionsMergeInternal(table,partitions,reserve_days,target_file_size_mb,"table",executor)
  }

  def mergeLeafPartition(table:String,partitionPath:String,targetFileCount:Int,mode:String="table",format:String):Unit={
    val partitionValues=partitionPath.split("/").filter(_.contains("="))
    val partitionSpecs=partitionValues.map(_.split("=")).map(array=> s"${array.head}='${array.last}'")
    val tempViewSuffix=partitionValues.map(_.split("=").last).mkString("_")
    val tempViewName=table.split("\\.").last+"_"+tempViewSuffix
    val df = spark.read.format(format).load(partitionPath).repartition(targetFileCount).checkpoint()
    df.createOrReplaceTempView(tempViewName)
    val mergeSql = s"""insert overwrite $table partition (${partitionSpecs.mkString(",")}) select * from $tempViewName"""
    val dirMergeSql = s"""insert overwrite DIRECTORY '$partitionPath' using $format  select * from $tempViewName"""

    def mergeByDir = {
      executeMergeSql(dirMergeSql)
      HdfsUtils.delete(partitionPath + "/_temporary")
    }

    mode match {
      case "table" =>
        try{
          executeMergeSql(mergeSql)
        }catch {
          case e=>
            mergeByDir
        }
      case "dir"=>
          mergeByDir
      case _=>
    }
      spark.catalog.dropTempView(tempViewName)
  }

  def executeMergeSql(sql:String): Unit ={
    log.warn("start: " +sql)
    spark.sql(sql)
    log.warn(s"executed: $sql")
  }

  override def process(dfList: ListBuffer[DataFrame]): DataFrame = {
    val table_col=config.getString(MergeSmallFilesProcessorConfig.table_col,"table_name")
    val reserve_days=config.getInt(MergeSmallFilesProcessorConfig.reserve_days,7)
    val target_file_size_mb=config.getInt(MergeSmallFilesProcessorConfig.target_file_size_mb,128)
    val max_dirs_per_group=config.getInt(MergeSmallFilesProcessorConfig.max_dirs_per_group,1000)
    val max_gb_per_group=config.getInt(MergeSmallFilesProcessorConfig.max_gb_per_group,25)
    val concurrent_merge_group_size=config.getInt(MergeSmallFilesProcessorConfig.concurrent_merge_group_size,5)
    val concurrent_static_merge_size=config.getInt(MergeSmallFilesProcessorConfig.concurrent_static_merge_size,24)
    val dynamic_merge_mode=config.getString(MergeSmallFilesProcessorConfig.dynamic_merge_mode,"dynamic")
    spark.sqlContext.setConf("spark.sql.adaptive.advisoryPartitionSizeInBytes",(target_file_size_mb*1024*1024).toString)
//    spark.sqlContext.setConf("spark.sql.adaptive.coalescePartitions.parallelismFirst","false")
    spark.sqlContext.setConf("spark.sql.adaptive.enabled","true")
    val inputDF = dfList(0)

    val tables=inputDF.collect()
    val table_index=new AtomicInteger(1)
      tables.foreach{
      r=>
        val table=r.getAs[String](table_col)
        log.warn(s"start merge table $table")
        spark.sparkContext.setJobGroup(s"$table(${table_index.getAndIncrement()}/${tables.size})",s"start merge table $table ,mode=$dynamic_merge_mode")
        if(dynamic_merge_mode.equalsIgnoreCase("dynamic"))
          {
            dynamicPartitionMerge(table, reserve_days, target_file_size_mb, max_dirs_per_group, max_gb_per_group, concurrent_merge_group_size,concurrent_static_merge_size)
          }
        else{
          staticPartitionMerge(table, reserve_days, target_file_size_mb, concurrent_static_merge_size)
        }

    }
    spark.emptyDataFrame
  }

  override def shortName(): String = "merge_small_files"
}

