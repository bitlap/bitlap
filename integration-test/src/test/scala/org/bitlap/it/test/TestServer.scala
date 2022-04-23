/* Copyright (c) 2022 bitlap.org */
package org.bitlap.it.test

import org.bitlap.Driver
import org.bitlap.it.EmbedBitlapServer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{ DriverManager, Statement }

class TestServer extends AnyFlatSpec with Matchers {

  "test server" should "ok" in {
    val server = new Thread {
      override def run(): Unit = EmbedBitlapServer.main(Array.empty)
    }
    server.setDaemon(true)
    server.start()
    Thread.sleep(3000L)

    Class.forName(classOf[Driver].getName)
    val con = DriverManager.getConnection("jdbc:bitlap://localhost:23333/default")
    val stmt: Statement = con.createStatement()

    // show databases
    stmt.execute("show databases")
    var rs = stmt.getResultSet
    rs.next()
    rs.getString("database_name") shouldBe "default"

    // show tables
    val table = "test_table"
    stmt.execute(s"create table if not exists $table")
    stmt.execute("show tables")
    rs = stmt.getResultSet
    rs.next()
    rs.getString("table_name") shouldBe table

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
    rs.getLong("_time") shouldBe 100
    rs.getDouble("vv") shouldBe 4
    rs.getDouble("pv") shouldBe 12
    rs.getLong("uv") shouldBe 3 // TODO: 兼容类型
  }
}
