/* Copyright (c) 2023 bitlap.org */
package org.bitlap

import org.bitlap.jdbc.Constants
import org.bitlap.network.ServerAddress

/** @author
 *    梦境迷离
 *  @version 1.0,2022/11/21
 */
package object client {

  private val Separator = ":"

  /** 从字符串解析IP:PORT，返回[[org.bitlap.network.ServerAddress]]
   */
  implicit final class StringOpsForClient(val serverUri: String) extends AnyVal {
    def asServerAddress: ServerAddress = {
      val as =
        if serverUri.contains(Separator) then serverUri.split(Separator).toList
        else List(serverUri, Constants.DEFAULT_PORT)
      ServerAddress(as.head.trim, as(1).trim.toIntOption.getOrElse(Constants.DEFAULT_PORT.toInt))
    }
  }

  /** 从字符串解析Array(IP:PORT,IP:PORT,IP:PORT,...)，返回[[org.bitlap.network.ServerAddress]]的列表
   */
  implicit final class ArrayStringOpsForClient(val serverPeers: Array[String]) extends AnyVal {
    def asServerAddresses: List[ServerAddress] =
      serverPeers.filter(_.nonEmpty).map(_.asServerAddress).toList
  }
}
