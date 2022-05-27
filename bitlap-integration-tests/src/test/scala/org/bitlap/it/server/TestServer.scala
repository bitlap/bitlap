/* Copyright (c) 2022 bitlap.org */
package org.bitlap.it.server

import junit.framework.TestCase
import org.bitlap.Driver
import org.bitlap.it.EmbedBitlapServer
import org.junit.Assert.assertEquals
import org.junit.Test

import java.sql.{ DriverManager, Statement }

class TestServer extends TestCase("TestServer") {

  def startServer(): Unit = {
    val server = new Thread {
      override def run(): Unit = EmbedBitlapServer.main(Array.empty)
    }
    server.setDaemon(true)
    server.start()
    Thread.sleep(3000L)
  }

  @Test
  def testServer {
    startServer()
    Class.forName(classOf[Driver].getName)
    val con             = DriverManager.getConnection("jdbc:bitlap://localhost:23333/default")
    val stmt: Statement = con.createStatement()

    // show databases
    stmt.execute("show databases")
    var rs = stmt.getResultSet
    rs.next()
    assertEquals(rs.getString("database_name"), "default")

    // show tables
    val table = "test_table"
    stmt.execute(s"create table if not exists $table")
    stmt.execute("show tables")
    rs = stmt.getResultSet
    rs.next()
    assertEquals(rs.getString("table_name"), table)

    // load data
    stmt.execute(s"load data 'classpath:simple_data.csv' overwrite table $table")

    // query
    stmt.execute(s"""
                    |select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
                    |from $table
                    |where _time = 100
                    |group by _time
                    |""".stripMargin)
    rs = stmt.getResultSet
    rs.next()
    assertEquals(rs.getLong("_time"), 100)
    assertEquals(rs.getDouble("vv"), 4, 0)
    assertEquals(rs.getDouble("pv"), 12, 0)
    assertEquals(rs.getLong("uv"), 3) // TODO: 兼容类型
  }
}
