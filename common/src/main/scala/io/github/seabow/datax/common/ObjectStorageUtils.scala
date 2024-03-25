package io.github.seabow.datax.common

import org.apache.hadoop.fs.FileStatus

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

  def deleteObjectStorageDir(dirPath:String,ec:ExecutionContext):Unit={
    Await.result(recurseDelete(HdfsUtils.getStatus(dirPath),ec),Duration.Inf)
  }

  def deleteObjectStorageDirs(dirPaths:Seq[String],ec:ExecutionContext):Unit={
    val delTasks=dirPaths.map(dirPath=>recurseDelete(HdfsUtils.getStatus(dirPath),ec))
    Await.result(Future.sequence(delTasks),Duration.Inf)
  }
}
