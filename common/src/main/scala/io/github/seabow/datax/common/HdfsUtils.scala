package io.github.seabow.datax.common

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import java.io.InputStream
import scala.util.Try

object HdfsUtils {
  implicit class StringImprovements (val s: String) {
    def toPath = new Path(s)
  }

  lazy val hdfs: FileSystem = FileSystem.get(SparkUtils.getHadoopConf())

  def readAsString(p: String): String = {
    val path = new Path(p)
    var result = new Array[Byte](0)
    if (hdfs.exists(path)) {
      val inputStream = hdfs.open(path)
      val stat = hdfs.getFileStatus(path)
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
    var result = new Array[Byte](0)
    if (hdfs.exists(path)) {
      val inputStream = hdfs.open(path)
      val stat = hdfs.getFileStatus(path)
      val length = stat.getLen.toInt
      val buffer = new Array[Byte](length)
      inputStream.readFully(buffer)
      result = buffer
    }
    result
  }

  def writeBytes(p:String,bytes:Array[Byte]):Boolean={
    val path=new Path(p)
    if(!hdfs.exists(path)){
      val dos = hdfs.create(path)
      dos.write(bytes, 0, bytes.length)
      dos.close()
    }
     true
  }

  def readAsInputStream(p: String): InputStream = {
    val path = new Path(p)
    if (hdfs.exists(path)) {
      hdfs.open(path)
    } else {
      null
    }
  }

  def getLatestSubdir(path: String): String = {
    Try{
      val lastestSubdir = hdfs.listStatus(new Path(path)).last.getPath.toString
      lastestSubdir
    }.getOrElse(path)
  }

  def listDirs(p: String): Array[FileStatus] = hdfs.listStatus(p.toPath).filter(_.isDirectory)

  def listFiles(p: String): Array[FileStatus] = hdfs.listStatus(p.toPath).filter(_.isFile)

  def exist(p: String): Boolean = hdfs.exists(p.toPath)

  def mkdir(path: String): Boolean = hdfs.mkdirs(path.toPath)

  def delete(path: String): Boolean = hdfs.delete(path.toPath, true)

  def status(path: String): Array[FileStatus] = hdfs.listStatus(path.toPath)

  def touch(path: String): Boolean = {
    if (!exist(path)) {
      hdfs.createNewFile(path.toPath)
      true
    }
    false
  }
}
