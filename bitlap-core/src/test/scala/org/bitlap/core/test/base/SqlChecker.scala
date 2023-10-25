/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.test.base

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.bitlap.common.utils.SqlEx
import org.bitlap.common.utils.SqlEx.toTable
import org.bitlap.common.utils.internal.DBTable
import org.bitlap.core.catalog.metadata.Database
import org.bitlap.core.sql.QueryExecution

import org.scalatest.matchers.should.Matchers._

/** Some sql check utils
 */
trait SqlChecker {

  /** execute sql statement
   */
  def sql(statement: String, session: SqlSession = SqlSession()): SqlResult = {
    val rs = QueryExecution(statement, session.currentSchema).execute()
    session.currentSchema = rs.currentSchema
    SqlResult(statement, SqlEx.toTable(rs.data))
  }

  /** check rows
   */
  def checkRows(statement: String, rows: List[List[Any]], session: SqlSession = SqlSession()): Unit = {
    val result = sql(statement, session).result
    try {
      result.size shouldBe rows.size
      result.zip(rows).foreach { case (r1, r2) =>
        r1.size shouldBe r2.size
        r1.zip(r2).foreach { case (v1, v2) =>
          v1 shouldBe v2
        }
      }
    } catch {
      case _: AssertionError =>
        fail(s"expected:<$rows>, but was:<$result>")
      case e =>
        throw e
    }
  }
}

class SqlResult(private val statement: String, private val table: DBTable) {

  lazy val result: List[List[Any]] = {
    val rs = ListBuffer[List[Any]]()
    (0 until table.getRowCount).foreach { r =>
      rs += table.getColumns.asScala.map(_.getTypeValues.get(r)).toList
    }
    rs.toList
  }

  lazy val size: Int = this.result.size

  def show(): SqlResult = {
    table.show()
    this
  }

  override def toString: String = {
    this.result.toString()
  }

  override def hashCode(): Int = {
    this.result.hashCode()
  }

  override def equals(other: Any): Boolean = {
    this.result == other
  }
}

final case class SqlSession(var currentSchema: String = Database.DEFAULT_DATABASE)
