/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.testkit

import java.sql.*

import org.bitlap.testkit.server.*

import org.junit.*
import org.scalatest.{ BeforeAndAfterAll, Inspectors }
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

import bitlap.rolls.core.jdbc.*

class ServerSpec extends AnyFunSuite with BeforeAndAfterAll with should.Matchers with Inspectors with CSVUtils {

  private lazy val table    = s"test_table_${FakeDataUtils.randEntityNumber}"
  private lazy val database = s"test_database_${FakeDataUtils.randEntityNumber}"

  Class.forName(classOf[org.bitlap.Driver].getName)
  given Connection = DriverManager.getConnection("jdbc:bitlap://localhost:23333/default")

  // Each test will be executed once and needs to be modified!
  val server = new Thread {
    override def run(): Unit = EmbedBitlapServer.main(scala.Array.empty)
  }

  override protected def beforeAll(): Unit = {
    server.setDaemon(true)
    server.start()
    Thread.sleep(3000L)

    initTable()
  }

  private def initTable(): Unit = {
    sql"create table if not exists $table"
    sql"load data 'classpath:simple_data.csv' overwrite table $table"
  }

  // Execute FakeDataUtilSpec to generate new mock data
  // When running Java 9 or above, JVM parameters are required: --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
  test("queryTest1") {
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
