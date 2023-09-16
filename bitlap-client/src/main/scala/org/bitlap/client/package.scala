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

/** Parsing IP:PORT from String, returning [[org.bitlap.network.ServerAddress]].
 */
extension (serverUri: String)

  def asServerAddress: ServerAddress = {
    val as =
      if serverUri.contains(Separator) then serverUri.split(Separator).toList
      else List(serverUri, Constants.DEFAULT_PORT)
    ServerAddress(as.head.trim, as(1).trim.toIntOption.getOrElse(Constants.DEFAULT_PORT.toInt))
  }
end extension

/** Parsing Array(IP:PORT,IP:PORT,IP:PORT,...) from String, returning a list of the
 *  [[org.bitlap.network.ServerAddress]].
 */
extension (serverPeers: Array[String])

  def asServerAddresses: List[ServerAddress] =
    serverPeers.filter(_.nonEmpty).map(_.asServerAddress).toList
end extension
