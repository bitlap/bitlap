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
package org.bitlap.server.session

import java.sql.*

import scala.collection.mutable.ListBuffer

import org.bitlap.common.exception.DataFormatException
import org.bitlap.core.*
import org.bitlap.core.sql.QueryExecution
import org.bitlap.network.enumeration.*
import org.bitlap.network.models.*
import org.bitlap.network.serde.BitlapSerde
import org.bitlap.server.config.BitlapConfiguration

import zio.*

/** Bitlap operation implementation on a single machine
 */
final class SimpleOperation(
  parentSession: Session,
  opType: OperationType,
  hasResultSet: Boolean = false
)(
  globalConfig: BitlapConfiguration)
    extends Operation(parentSession, opType, hasResultSet, globalConfig)
    with BitlapSerde {

  def mapTo(rs: ResultSet): QueryResultSet = {
    // get schema
    val metaData = rs.getMetaData
    val columns = (1 to metaData.getColumnCount).map { it =>
      val colName = metaData.getColumnName(it)
      val colType = TypeId.jdbc2Bitlap.getOrElse(
        metaData.getColumnType(it),
        TypeId.StringType
      ) // TODO temporarily, do not use TypeId.Unspecified to avoid errors
      ColumnDesc(colName, colType)
    }
    // get row set
    val rows = ListBuffer[Row]()
    while rs.next() do {
      val cl = (1 to metaData.getColumnCount).map { it =>
        metaData.getColumnType(it) match {
          case Types.VARCHAR                => serialize(rs.getString(it))
          case Types.SMALLINT               => serialize(rs.getShort(it))
          case Types.TINYINT                => serialize(rs.getByte(it))
          case Types.INTEGER                => serialize(rs.getInt(it))
          case Types.BIGINT | Types.NUMERIC => serialize(rs.getLong(it))
          case Types.DOUBLE                 => serialize(rs.getDouble(it))
          case Types.BOOLEAN                => serialize(rs.getBoolean(it))
          case Types.TIMESTAMP              => serialize(rs.getLong(it))
          case Types.FLOAT                  => serialize(rs.getFloat(it))
          case Types.TIME                   => serialize(rs.getTime(it).getTime)
          case Types.DATE                   => serialize(rs.getDate(it).getTime)
          case tp                           => throw DataFormatException(s"Unsupported type:$tp")
        }
      }
      rows.append(Row(cl.toList))
    }
    QueryResultSet(
      TableSchema(columns.toList),
      RowSet(rows.toList)
    )
  }

  override def run(): Task[Unit] = {
    for {
      _ <- ZIO.attemptBlocking(super.setState(OperationState.RunningState))
      currentSchemaRef <- parentSession.currentSchemaRef.getAndUpdate { schema =>
        try {
          val execution = new QueryExecution(statement, schema.get()).execute()
          execution match
            case DefaultQueryResult(data, currentSchema) =>
              schema.set(currentSchema)
              cache.put(opHandle, this.mapTo(data))
            case _ =>
          super.setState(OperationState.FinishedState)
        } catch {
          case e: Exception =>
            super.setState(OperationState.ErrorState)
            throw e
        }
        schema
      }
    } yield ()
  }

}
