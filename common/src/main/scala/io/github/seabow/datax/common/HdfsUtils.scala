package io.github.seabow.datax.common

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import java.io.InputStream
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

  def exist(p: String): Boolean = hdfs(p.toPath).exists(p.toPath)

  def mkdir(path: String): Boolean = hdfs(path.toPath).mkdirs(path.toPath)

  def delete(path: String): Boolean = hdfs(path.toPath).delete(path.toPath, true)

  def status(path: String): Array[FileStatus] = hdfs(path.toPath).listStatus(path.toPath)

  def touch(path: String): Boolean = {
    if (!exist(path)) {
      hdfs(path.toPath).createNewFile(path.toPath)
      true
    }
    false
  }
}
