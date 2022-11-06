/* Copyright (c) 2022 bitlap.org */
package org.bitlap

import java.sql.{ Connection, ResultSet }

/** @since 2022/11/6
 *  @author
 *    梦境迷离
 */
package object testkit {

  Class.forName(classOf[org.bitlap.Driver].getName)

  implicit final class StringOps(val sqlStatement: StringContext) extends AnyVal {
    def sql(args: Any*)(implicit connection: Connection): ResultSet = {
      // 检查
      val stmt = implicitly[Connection].createStatement()
      stmt.executeQuery(sqlStatement.s(args: _*))
    }
  }
}
