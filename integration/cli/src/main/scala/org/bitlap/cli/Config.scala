package org.bitlap.cli

import org.backuity.clist._

/**
 * @author 梦境迷离
 * @since 2021/12/12
 * @version 1.0
 * @param sql The sql which can contains "${argsName}", but args should pass into by kwargs
 * @param kwargs
 */
case class Config(cmd: String = "", sql: String = "", kwargs: Map[String, String] = Map.empty)

class SQL extends Command(description = "cli SQL command") {
  var s: List[String] = args[List[String]]()
  // TODO 参数有问题
  var kvargs: String = opt[String](default = "")
}