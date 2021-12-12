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

class bql extends Command(description = "Bitlap sql command") {
  var sql: List[String] = args[List[String]]()
  //--kvargs=k1=v1,k2=v2
  var kvargs: String = opt[String](default = "", description = "Statement SQL parameters to replace \"?\", e.g. key1=value1,key2=value2")
}