/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit

import org.bitlap.common.jdbc._
import org.bitlap.testkit.server._
import org.junit._

import java.sql._

class ServerSpec extends CsvUtil {

  private lazy val table    = s"test_table_${FakeDataUtil.randEntityNumber}"
  private lazy val database = s"test_database_${FakeDataUtil.randEntityNumber}"

  implicit lazy val conn: Connection = DriverManager.getConnection("jdbc:bitlap://localhost:23333/default")

  // 每个测试都会执行一次，需要修改！
  @Before
  def startServer(): Unit = {
    val server = new Thread {
      override def run(): Unit = EmbedBitlapServer.main(scala.Array.empty)
    }
    server.setDaemon(true)
    server.start()
    Thread.sleep(3000L)

    initTable()
  }

  private def initTable(): Unit = {
    sql"create table if not exists $table"
    sql"load data 'classpath:simple_data.csv' overwrite table $table"
  }

  @After
  def dropTable(): Unit =
    sql"drop table $table cascade"

  // 执行FakeDataUtilSpec 生成新的mock数据
  // 在java 9以上运行时，需要JVM参数：--add-exports java.base/jdk.internal.ref=ALL-UNNAMED
  @Test
  def query_test1() {
    val rs = sql"""
       select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
       from $table
       where _time >= 0
       group by _time
       """
    val ret1 = ResultSetTransformer[GenericRow4[Long, Double, Double, Long]].toResults(rs)
    assert(ret1.nonEmpty)

    sql"create database if not exists $database"
    sql"use $database"

    val showResult = ResultSetTransformer[GenericRow1[String]].toResults(sql"show current_database")
    println(database)
    println(showResult.map(_.col1))
    assert(showResult.nonEmpty && showResult.exists(_.col1 == database))
  }

}
