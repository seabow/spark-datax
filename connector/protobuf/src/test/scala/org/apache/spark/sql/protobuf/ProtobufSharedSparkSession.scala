package org.apache.spark.sql.protobuf

import org.apache.spark.SparkConf
import org.apache.spark.sql.test.SharedSparkSession

trait ProtobufSharedSparkSession extends SharedSparkSession{
  override def sparkConf: SparkConf = {
    val conf=super.sparkConf
     conf.set("spark.sql.extensions", "org.apache.spark.sql.extensions.ProtobufExtensions")
  }
}
