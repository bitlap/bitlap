/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc

import com.google.protobuf.ByteString
import org.bitlap.core.sql.QueryExecution
import org.bitlap.network.{ models, OperationType }
import org.bitlap.tools.apply

import java.sql.{ ResultSet, Types }
import scala.collection.mutable.ListBuffer

/** @author
 *    梦境迷离
 *  @version 1.0,2021/12/3
 */
@apply
class BitlapOperation(
  parentSession: Session,
  opType: OperationType,
  hasResultSet: Boolean = false
) extends Operation(parentSession, opType, hasResultSet) {

  def wrapper(rs: ResultSet): models.QueryResult = {
    // get schema
    val metaData = rs.getMetaData
    val columns = (1 to metaData.getColumnCount).map { it =>
      val colName = metaData.getColumnName(it)
      val colType = metaData.getColumnType(it) match {
        case Types.VARCHAR   => models.TypeId.StringType
        case Types.SMALLINT  => models.TypeId.ShortType
        case Types.INTEGER   => models.TypeId.IntType
        case Types.BIGINT    => models.TypeId.LongType
        case Types.DOUBLE    => models.TypeId.DoubleType
        case Types.BOOLEAN   => models.TypeId.BooleanType
        case Types.TIMESTAMP => models.TypeId.TimestampType
        case Types.TINYINT   => models.TypeId.ByteType
        // TODO more 暂时不使用TYPE_ID_UNSPECIFIED，避免报错
        case _ => models.TypeId.StringType
      }
      models.ColumnDesc(colName, colType)
    }
    // get row set
    val rows = ListBuffer[models.Row]()
    while (rs.next()) {
      val cl = (1 to metaData.getColumnCount).map { it =>
        metaData.getColumnType(it) match {
          case Types.VARCHAR                => ByteString.copyFromUtf8(rs.getString(it))
          case Types.SMALLINT               => ByteString.copyFromUtf8(rs.getShort(it).toString)
          case Types.TINYINT                => ByteString.copyFromUtf8(rs.getByte(it).toString)
          case Types.INTEGER                => ByteString.copyFromUtf8(rs.getInt(it).toString)
          case Types.BIGINT | Types.NUMERIC => ByteString.copyFromUtf8(rs.getLong(it).toString)
          case Types.DOUBLE                 => ByteString.copyFromUtf8(rs.getDouble(it).toString)
          case Types.BOOLEAN                => ByteString.copyFromUtf8(rs.getBoolean(it).toString)
          case Types.TIMESTAMP              => ByteString.copyFromUtf8(rs.getLong(it).toString)
          // TODO more
          case _ => ByteString.empty()
        }
      }
      rows.append(models.Row(cl.toList))
    }
    models.QueryResult(
      models.TableSchema(columns.toList),
      models.RowSet(rows.toList)
    )
  }

  override def run(): Unit =
    cache.put(
      super.getOpHandle,
      wrapper(new QueryExecution(super.getStatement).execute()) // TODO: add error
    )

}
