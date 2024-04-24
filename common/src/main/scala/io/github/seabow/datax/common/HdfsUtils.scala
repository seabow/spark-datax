package io.github.seabow.datax.common

import org.apache.hadoop.fs.{ContentSummary, FileStatus, FileSystem, Path}

import java.io.InputStream
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

object HdfsUtils {
  implicit class StringImprovements (val s: String) {
    def toPath = new Path(s)
  }

  def hdfs(path: Path): FileSystem = FileSystem.get(path.toUri,SparkUtils.getHadoopConf())

  def readAsString(p: String): String = {
    val path = new Path(p)
     val fs=hdfs(path)
    var result = new Array[Byte](0)
    if (fs.exists(path)) {
      val inputStream = fs.open(path)
      val stat = fs.getFileStatus(path)
      val length = stat.getLen.toInt
      val buffer = new Array[Byte](length)
      inputStream.readFully(buffer)
      inputStream.close()
      result = buffer
    }
    result.map(_.toChar).mkString
  }

  def readAsByte(p: String): Array[Byte] = {
    val path = new Path(p)
    val fs=hdfs(path)

    var result = new Array[Byte](0)
    if (fs.exists(path)) {
      val inputStream = fs.open(path)
      val stat = fs.getFileStatus(path)
      val length = stat.getLen.toInt
      val buffer = new Array[Byte](length)
      inputStream.readFully(buffer)
      result = buffer
    }
    result
  }

  def writeBytes(p:String,bytes:Array[Byte]):Boolean={
    val path=new Path(p)
    val fs=hdfs(path)
    if(!fs.exists(path)){
      val dos = fs.create(path)
      dos.write(bytes, 0, bytes.length)
      dos.close()
    }
     true
  }

  def readAsInputStream(p: String): InputStream = {
    val path = new Path(p)
    val fs=hdfs(path)
    if (fs.exists(path)) {
      fs.open(path)
    } else {
      null
    }
  }

  def getLatestSubdir(path: String): String = {
    Try{
      val p=new Path(path)
      val fs=hdfs(p)
      val lastestSubdir = fs.listStatus(p).last.getPath.toString
      lastestSubdir
    }.getOrElse(path)
  }

  def listDirs(p: String): Array[FileStatus] = hdfs(p.toPath).listStatus(p.toPath).filter(_.isDirectory)

  def listFiles(p: String): Array[FileStatus] = hdfs(p.toPath).listStatus(p.toPath).filter(_.isFile)

  def listStatus(p: String): Array[FileStatus] = hdfs(p.toPath).listStatus(p.toPath)

  def exist(p: String): Boolean = hdfs(p.toPath).exists(p.toPath)

  def mkdir(path: String): Boolean = hdfs(path.toPath).mkdirs(path.toPath)

  def delete(path: String): Boolean = hdfs(path.toPath).delete(path.toPath, true)

  def status(path: String): Array[FileStatus] = hdfs(path.toPath).listStatus(path.toPath)

  def getContentSummary(path: String): ContentSummary = hdfs(path.toPath).getContentSummary(path.toPath)

  def getStatus(path: String): FileStatus = hdfs(path.toPath).getFileStatus(path.toPath)

  def touch(path: String): Boolean = {
    if (!exist(path)) {
      hdfs(path.toPath).createNewFile(path.toPath)
      true
    }
    false
  }

  def getContentSummaryWithThreads(numThreads:Int)(path: String): ContentSummary = {
    val status = this.getStatus(path)
    if (status.isFile) {
      val length = status.getLen
      (new ContentSummary.Builder).length(length).fileCount(1L).directoryCount(0L).spaceConsumed(length).build
    }
    else {
      val summary = Array[AtomicLong](new AtomicLong(0l), new AtomicLong(0l), new AtomicLong(1l))
      val children = this.listStatus(path)
      val ec= FutureUtils.buildExecutorContext(numThreads)
      val futureTasks:ListBuffer[Future[Any]]=ListBuffer.empty[Future[Any]]
      children.foreach{
        child=>
         val future=Future{
          val c= if (child.isDirectory) this.getContentSummary(child.getPath.toString)
           else (new ContentSummary.Builder).length(child.getLen).fileCount(1L).directoryCount(0L).spaceConsumed(child.getLen).build
           summary(0).addAndGet(c.getLength)
           summary(1).addAndGet(c.getFileCount)
           summary(2).addAndGet(c.getDirectoryCount)
         }(ec)
          futureTasks.append(future)
      }
      Await.result(Future.sequence(futureTasks),Duration.Inf)
      (new ContentSummary.Builder).length(summary(0).get()).fileCount(summary(1).get()).directoryCount(summary(2).get()).spaceConsumed(summary(0).get()).build
    }
  }
}
