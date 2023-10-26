/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
      assert(result.size == rows.size)
      result.zip(rows).foreach { case (r1, r2) =>
        assert(r1.size == r2.size)
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
