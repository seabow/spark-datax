package io.github.seabow.datax.common

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

case class TableInfo(partitionSpec:String,location:String,provider:String)

object HiveUtils {
  def getPartitionSpecAndLocation(table:String):(String,String)={
    val spark=SparkSession.getActiveSession.get
    val describeDF=spark.sql(s"describe EXTENDED $table")
    val location=describeDF.collect().filter(_.getAs[String]("col_name").equals("Location")).head.getAs[String]("data_type")
    var startCollectPartitionColsFlag=false
    var partitionSpec=ListBuffer.empty[String]
    describeDF.collect().foreach{
      r=>
        if(r.getAs[String]("col_name").equals("# Detailed Table Information")){
          startCollectPartitionColsFlag=false
        }
        if(startCollectPartitionColsFlag){
          partitionSpec.append(r.getAs[String]("col_name"))
        }
        if(r.getAs[String]("col_name").equals("# col_name")){
          startCollectPartitionColsFlag=true
        }
    }
    (partitionSpec.filter(_.nonEmpty).mkString(","),location)
  }

  def getProvider(table:String):String={
    val spark=SparkSession.active
    val describeInfo=spark.sql(s"describe EXTENDED $table").collect()
    val provider=describeInfo.filter(_.getAs[String]("col_name").equals("Provider")).head.getAs[String]("data_type")
    provider
  }

}
