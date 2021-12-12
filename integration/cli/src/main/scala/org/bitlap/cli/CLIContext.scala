package org.bitlap.cli

/**
 * 执行上下文
 * TODO result
 *
 * @author 梦境迷离
 * @since 2021/12/12
 * @version 1.0
 */
case class CLIContext(sql: String, kvargs: Map[String, String] = Map.empty)
