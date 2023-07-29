/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.client

import org.bitlap.jdbc.Constants
import org.bitlap.network.ServerAddress

/** @author
 *    梦境迷离
 *  @version 1.0,2022/11/21
 */

private val Separator = ":"

/** 从字符串解析IP:PORT，返回[[org.bitlap.network.ServerAddress]]
 */
extension (serverUri: String)

  def asServerAddress: ServerAddress = {
    val as =
      if serverUri.contains(Separator) then serverUri.split(Separator).toList
      else List(serverUri, Constants.DEFAULT_PORT)
    ServerAddress(as.head.trim, as(1).trim.toIntOption.getOrElse(Constants.DEFAULT_PORT.toInt))
  }
end extension

/** 从字符串解析Array(IP:PORT,IP:PORT,IP:PORT,...)，返回[[org.bitlap.network.ServerAddress]]的列表
 */
extension (serverPeers: Array[String])

  def asServerAddresses: List[ServerAddress] =
    serverPeers.filter(_.nonEmpty).map(_.asServerAddress).toList
end extension
