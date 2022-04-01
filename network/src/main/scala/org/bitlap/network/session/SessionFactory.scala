/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network.session

/**
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
trait SessionFactory {

  def create(
    username: String,
    password: String,
    sessionConf: Map[String, String],
    sessionManager: SessionManager
  ): Session

}
