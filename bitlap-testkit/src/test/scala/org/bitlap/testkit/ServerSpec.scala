/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.testkit

import java.sql.*

import org.bitlap.testkit.server.*

import org.junit.*

import bitlap.rolls.core.jdbc.*

class ServerSpec extends CSVUtils {

  private lazy val table    = s"test_table_${FakeDataUtils.randEntityNumber}"
  private lazy val database = s"test_database_${FakeDataUtils.randEntityNumber}"

  Class.forName(classOf[org.bitlap.Driver].getName)
  given Connection = DriverManager.getConnection("jdbc:bitlap://localhost:23333/default")

  // 每个测试都会执行一次，需要修改！
  val server = new Thread {
    override def run(): Unit = EmbedBitlapServer.main(scala.Array.empty)
  }

  @Before
  def startServer(): Unit = {
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
  // 在java 9以上运行时，需要JVM参数: --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
  @Test
  def query_test1(): Unit = {
    val rs = sql"""
       select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
       from $table
       where _time >= 0
       group by _time
       """
    val ret1 = ResultSetX[TypeRow5[Long, Long, String, String, Int]](rs).fetch()
    assert(ret1.nonEmpty)
    println(ret1)

    //    sql"create database if not exists $database"
    //    sql"use $database"

    //    val showResult = ResultSetX[TypeRow1[String]](sql"show current_database").fetch()
    //    println(database)
    //    println(showResult.map(_.values))
    //    assert(showResult.nonEmpty && showResult.exists(_.values.contains(database)))
  }

}
