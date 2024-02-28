package org.apache.spark.datax.utils

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.ReliableRDDCheckpointData

object CheckPointCleanUtils extends Logging{
   def cleanCheckpoint(rddId:Int):Unit={
     val sc=SparkContext.getActive
     try {
       log.warn("Cleaning rdd checkpoint data " + rddId)
       ReliableRDDCheckpointData.cleanCheckpoint(sc.get, rddId)
       log.warn("Cleaned rdd checkpoint data " + rddId)
     }
     catch {
       case e: Exception => logError("Error cleaning rdd checkpoint data " + rddId, e)
     }
   }
}
