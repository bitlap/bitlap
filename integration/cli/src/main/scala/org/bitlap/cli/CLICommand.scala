package org.bitlap.cli

/**
 *
 * @author 梦境迷离
 * @since 2021/12/12
 * @version 1.0
 */
sealed trait CLICommand

object CLICommand {
  case class ExecuteStatement(sql: String, args: Map[String,String] = Map.empty) extends CLICommand

  case class ExecuteDDL(sql: String, args: Map[String,String] = Map.empty) extends CLICommand

  case class ExecuteNativeSQL(sql: String, args: Map[String,String] = Map.empty) extends CLICommand

  case object Invalid extends CLICommand
}
