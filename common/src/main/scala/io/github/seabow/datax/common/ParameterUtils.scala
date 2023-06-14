package io.github.seabow.datax.common

object ParameterUtils {
  val usage =""

  /**
   * 获取脚本传入的参数
   * @param args
   * @return
   */
  def getParameters(args: Array[String]): Map[String, String] = {
    var options = Map[String, String]()
    args.foreach(item => {
      if (item == "-h" || item == "-help" | item == "--help") {
        println(usage)
        System.exit(0)
      }
      val strList = item.split("=")
      if (strList.length == 2) {
        val key = strList(0)
        val value = strList(1)
        //        if (optionalParameter.contains(key)) {
        options += (key -> value)
        //        }
      }
      if(item.split("=").length > 2){
        val key = item.substring(0,item.indexOf("="))
        val value = item.substring(item.indexOf("=") + 1)
        options += (key -> value)
      }
    })
    if (options.nonEmpty) {
      print("Used Options: ")
      options.keys.foreach(i => {
        print("  ")
        print(i)
        print(": ")
        print(options(i))
      })
      println()
    } else {
      println("Unused Parameter!")
    }
    options
  }

  def dealIndicatorStr(str:String): List[String] = {
    var flag = 0;
    val res = new StringBuffer
    for (i <- 0 until str.length) {
      val ch = str.charAt(i)
      if (ch == '(') flag += 1
      if (ch == ')') flag -= 1
      if (flag > 0) {
        res.append(ch.toString.replace(",", "\u0001"))
      } else {
        res.append(ch)
      }
    }
    res.toString.split(",").map(_.replace("\u0001",",")).toList
  }
}
