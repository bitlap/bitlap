/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.session

import com.google.protobuf.ByteString
import org.bitlap.core.sql.QueryExecution
import org.bitlap.network.OperationType
import org.bitlap.network.models._
import org.bitlap.tools.apply

import java.sql._
import scala.collection.mutable.ListBuffer

/** bitlap 客户端操作
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2021/12/3
 */
@apply
class MemoryOperation(
  parentSession: Session,
  opType: OperationType,
  hasResultSet: Boolean = false
) extends Operation(parentSession, opType, hasResultSet) {

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
          // TODO 现在所有类型都使用ByteString传输，后续定义protobuf数据类型映射SQL类型？
          case Types.VARCHAR                => ByteString.copyFromUtf8(rs.getString(it)) // FIXME 使用更合理的序列化方式
          case Types.SMALLINT               => ByteString.copyFromUtf8(rs.getShort(it).toString)
          case Types.TINYINT                => ByteString.copyFromUtf8(rs.getByte(it).toString)
          case Types.INTEGER                => ByteString.copyFromUtf8(rs.getInt(it).toString)
          case Types.BIGINT | Types.NUMERIC => ByteString.copyFromUtf8(rs.getLong(it).toString)
          case Types.DOUBLE                 => ByteString.copyFromUtf8(rs.getDouble(it).toString)
          case Types.BOOLEAN                => ByteString.copyFromUtf8(rs.getBoolean(it).toString)
          case Types.TIMESTAMP              => ByteString.copyFromUtf8(rs.getLong(it).toString)
          case Types.FLOAT                  => ByteString.copyFromUtf8(rs.getFloat(it).toString)
          case Types.TIME                   => ByteString.copyFromUtf8(rs.getTime(it).getTime.toString)
          case Types.DATE                   => ByteString.copyFromUtf8(rs.getDate(it).getTime.toString)
          case _                            => ByteString.empty()
        }
      }
      rows.append(Row(cl.toList))
    }
    QueryResult(
      TableSchema(columns.toList),
      RowSet(rows.toList)
    )
  }

  override def run(): Unit =
    cache.put(
      super.getOpHandle,
      mapTo(new QueryExecution(super.getStatement).execute()) // TODO: add error
    )

}
