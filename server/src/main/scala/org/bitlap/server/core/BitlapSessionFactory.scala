package org.bitlap.server.core

import org.bitlap.net.session.SessionFactory
import org.bitlap.net.session.{ Session, SessionManager }

/**
 *
 * @author 梦境迷离
 * @version 1.0,2021/12/3
 */
class BitlapSessionFactory extends SessionFactory{

  override def create(username: String, password: String, sessionConf: Map[String, String], sessionManager: SessionManager): Session = {
    new BitlapSession(username, password, sessionConf, sessionManager)
  }
}
