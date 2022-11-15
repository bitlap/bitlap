/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.session

import com.google.protobuf.ByteString
import org.bitlap.core.sql.QueryExecution
import org.bitlap.network._
import org.bitlap.network.models._
import org.bitlap.tools.apply

import java.sql._
import scala.collection.mutable.ListBuffer
import org.bitlap.core._
import org.bitlap.network.NetworkException.DataFormatException

/** bitlap 客户端操作
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2021/12/3
 */
@apply
final class MemoryOperation(
  parentSession: Session,
  opType: OperationType,
  hasResultSet: Boolean = false
) extends Operation(parentSession, opType, hasResultSet)
    with BitlapSerde {

  def mapTo(rs: ResultSet): QueryResult = {
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
    while (rs.next()) {
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
    QueryResult(
      TableSchema(columns.toList),
      RowSet(rows.toList)
    )
  }

  override def run(): Unit = {
    super.setState(OperationState.RunningState)
    cache.put(
      super.getOpHandle,
      mapTo(
        new QueryExecution(
          super.getStatement,
          new SessionId(parentSession.sessionHandle.handleId)
        ).execute()
      )
    )
  }

}
