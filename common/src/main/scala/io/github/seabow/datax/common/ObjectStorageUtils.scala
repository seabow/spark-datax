package io.github.seabow.datax.common

import org.apache.hadoop.fs.{ContentSummary, FileStatus, Path}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
/**
 *  As some object storages works bad in remove files. This class give some resolution:
 *  recursely list and cleanup files in dirs concurrently.
 */
object ObjectStorageUtils {
  private def recurseDelete(status: FileStatus,ec:ExecutionContext): Future[Any] = {
    if (status.isDirectory) {
      if (status.getLen > 0) {
        val eventualUnits = HdfsUtils.listStatus(status.getPath.toString).map {
          status =>
            recurseDelete(status,ec)
        }
        Await.result(Future.sequence(eventualUnits.toSeq), scala.concurrent.duration.Duration.Inf)
      }
      Future{
        HdfsUtils.delete(status.getPath.toString)
        println(s"Deleted ${status.getPath.toString}")
      }(ec)
    }
    else {
      Future {
        HdfsUtils.delete(status.getPath.toString);
        println(s"Deleted ${status.getPath.toString}")
      }(ec)
    }
  }

  def recurseCopy(srcStatus:FileStatus,dstPath:String,ec:ExecutionContext): Future[Any]= {
    val futureTasks=ListBuffer.empty[Future[Any]]
    val srcPath=srcStatus.getPath.toString
    if (srcStatus.isDirectory) {
      val subStatuses=HdfsUtils.listStatus(srcStatus.getPath.toString)
      if (subStatuses.length>0){
       subStatuses.foreach {
          status =>
            recurseCopy(status,dstPath+Path.SEPARATOR+status.getPath.getName,ec)
         }
        }
    }else{
     val future= Future{
       if(HdfsUtils.exist(dstPath)) {
           val len=HdfsUtils.getStatus(dstPath).getLen
           if(HdfsUtils.getStatus(srcPath).getLen!=len){
             HdfsUtils.copy(srcPath,dstPath)
           }
         }else{
         HdfsUtils.copy(srcPath,dstPath)
       }
       println(s"copied $srcPath -> $dstPath")
      }(ec)
      futureTasks.append(future)
    }
    Future.sequence(futureTasks)
  }

  def deleteObjectStorageDir(dirPath:String,ec:ExecutionContext):Unit={
    Await.result(recurseDelete(HdfsUtils.getStatus(dirPath),ec),Duration.Inf)
  }

  def deleteObjectStorageDirs(dirPaths:Seq[String],ec:ExecutionContext):Unit={
    val delTasks=dirPaths.map(dirPath=>recurseDelete(HdfsUtils.getStatus(dirPath),ec))
    Await.result(Future.sequence(delTasks),Duration.Inf)
  }

  def  copyObjectStorageDir(srcDir:String,dstDir:String,executionContext: ExecutionContext):Unit={
    Await.result(recurseCopy(HdfsUtils.getStatus(srcDir),dstDir,executionContext),Duration.Inf)
  }
}