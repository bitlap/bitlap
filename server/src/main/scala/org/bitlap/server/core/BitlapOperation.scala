package org.bitlap.server.core

import org.bitlap.net.operation.OperationType.OperationType
import org.bitlap.net.operation.operations
import org.bitlap.net.session.Session

import org.bitlap.net.models
import java.sql.Types
import com.google.protobuf.ByteString
import java.sql.ResultSet
import scala.collection.mutable.ListBuffer
import org.bitlap.core.sql.QueryExecution

/**
 *
 * @author 梦境迷离
 * @version 1.0,2021/12/3
 */
class BitlapOperation(
  parentSession: Session, opType: OperationType, hasResultSet: Boolean = false)
  extends operations.Operation(parentSession, opType, hasResultSet) {

  def wrapper(rs: ResultSet): models.QueryResult = {
    // get schema
    val metaData = rs.getMetaData
    val columns = (1 to metaData.getColumnCount).map { it =>
      val colName = metaData.getColumnName(it)
      val colType = metaData.getColumnType(it) match {
        case Types.VARCHAR => models.TypeId.B_TYPE_ID_STRING_TYPE
        case Types.INTEGER => models.TypeId.B_TYPE_ID_INT_TYPE
        case Types.DOUBLE => models.TypeId.B_TYPE_ID_DOUBLE_TYPE
        // TODO more
        case _ => models.TypeId.B_TYPE_ID_UNSPECIFIED
      }
      models.ColumnDesc(colName, colType)
    }
    // get row set
    val rows = ListBuffer[models.Row]()
    while (rs.next()) {
      val cl = (1 to metaData.getColumnCount).map { it =>
        metaData.getColumnType(it) match {
          case Types.VARCHAR => ByteString.copyFromUtf8(rs.getString(it))
          // TODO more
          case _ => ByteString.copyFrom(rs.getBytes(it))
        }
      }
      rows.append(models.Row(cl.toList))
    }
    models.QueryResult(models.TableSchema(columns.toList), models.RowSet(rows.toList))
  }

  override def run(): Unit = {
    cache.put(super.getOpHandle, wrapper(new QueryExecution(super.getStatement).execute()))

  }
}

object BitlapOperation {
  def apply(parentSession: Session, opType: OperationType, hasResultSet: Boolean = false): BitlapOperation = {
    new BitlapOperation(parentSession, opType, hasResultSet)
  }
}