/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.session

import java.sql.*

import scala.collection.mutable.ListBuffer

import org.bitlap.core.*
import org.bitlap.core.sql.QueryExecution
import org.bitlap.network.NetworkException.DataFormatException
import org.bitlap.network.enumeration.*
import org.bitlap.network.models.*
import org.bitlap.network.serde.BitlapSerde

/** bitlap 单机操作实现
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2021/12/3
 */
final class SimpleOperation(parentSession: Session, opType: OperationType, hasResultSet: Boolean = false)
    extends Operation(parentSession, opType, hasResultSet)
    with BitlapSerde {

  def mapTo(rs: ResultSet): QueryResultSet = {
    // get schema
    val metaData = rs.getMetaData
    val columns = (1 to metaData.getColumnCount).map { it =>
      val colName = metaData.getColumnName(it)
      val colType = TypeId.jdbc2Bitlap.getOrElse(
        metaData.getColumnType(it),
        TypeId.StringType
      ) // TODO more 暂时不使用TypeId.Unspecified，避免报错
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
          case tp                           => throw DataFormatException(msg = s"Unsupported type:$tp")
        }
      }
      rows.append(Row(cl.toList))
    }
    QueryResultSet(
      TableSchema(columns.toList),
      RowSet(rows.toList)
    )
  }

  override def run(): Unit = {
    super.setState(OperationState.RunningState)
    try {
      val execution = new QueryExecution(statement, parentSession.currentSchema).execute()
      parentSession.currentSchema = execution.getCurrentSchema // reset current schema
      cache.put(opHandle, mapTo(execution.getData))
      super.setState(OperationState.FinishedState)
    } catch {
      case e: Exception =>
        super.setState(OperationState.ErrorState)
        throw e
    }
  }

}
