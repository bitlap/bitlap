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

  implicit final class StringOpsForClient(val s: String) extends AnyVal {
    def extractServerAddress: ServerAddress = {
      val as = if (s.contains(Separator)) s.split(Separator).toList else List(s, Constants.DEFAULT_PORT)
      ServerAddress(as.head.trim, as(1).trim.toIntOption.getOrElse(Constants.DEFAULT_PORT.toInt))
    }
  }

  def serverAddresses(serverPeers: Array[String]): List[ServerAddress] =
    serverPeers.filter(_.nonEmpty).map(_.extractServerAddress).toList
}
