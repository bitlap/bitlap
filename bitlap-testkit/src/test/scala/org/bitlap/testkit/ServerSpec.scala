/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit

import org.bitlap.common.jdbc._
import org.bitlap.testkit.server._
import org.junit._

import java.sql._

class ServerSpec extends CsvHelper {

  val table = s"test_table"

  private def startServer(): Unit = {

    val server = new Thread {
      override def run(): Unit = EmbedBitlapServer.main(scala.Array.empty)
    }
    server.setDaemon(true)
    server.start()
    Thread.sleep(3000L)
  }

  private def initTable(): Unit = {
    val stmt = conn.createStatement()
    stmt.execute(s"create table if not exists $table")
    stmt.execute(s"load data 'classpath:simple_data.csv' overwrite table $table") // load的是server模块的csv
  }

  def conn: Connection = {
    Class.forName(classOf[org.bitlap.Driver].getName)
    DriverManager.getConnection("jdbc:bitlap://localhost:23333/default")
  }

  // 执行FakeDataUtilSpec 生成新的mock数据
  // 在java 9以上运行时，需要JVM参数：--add-exports java.base/jdk.internal.ref=ALL-UNNAMED
  @Test
  def query_test1() {
    startServer()
    initTable()
    val stmt = conn.createStatement()
    stmt.setMaxRows(10)
    stmt.execute(s"""
                    |select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
                    |from $table
                    |where _time >= 0
                    |group by _time
                    |""".stripMargin)
    val rs   = stmt.getResultSet
    val ret1 = ResultSetTransformer[GenericRow4[Long, Double, Double, Long]].toResults(rs)
    assert(ret1.nonEmpty)
  }
}
