package org.apache.spark

import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.SparkHadoopUtil

object SparkHadoopUtilProxy {
  def newConfiguration: Configuration = {SparkHadoopUtil.newConfiguration(SparkEnv.get.conf)}
}
