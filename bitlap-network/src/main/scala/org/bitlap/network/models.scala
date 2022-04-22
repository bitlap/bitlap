/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import com.google.protobuf.ByteString
import org.bitlap.network.driver.proto._
import org.bitlap.network.models.TypeId.TypeId

/**
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
object models {

  sealed trait Model

  final case class QueryResult(tableSchema: TableSchema, rows: RowSet) extends Model

  final case class RowSet(rows: List[Row] = Nil, startOffset: Long = 0) extends Model {
    def toBRowSet: BRowSet = BRowSet(startRowOffset = startOffset, rows = rows.map(_.toBRow))
  }

  /**
   * The wrapper class of the Proto buffer `BRow`.
   */
  final case class Row(private val values: List[ByteString] = Nil) extends Model {
    def toBRow: BRow = BRow(values)
  }

  final case class TableSchema(private val columns: List[ColumnDesc] = Nil) extends Model {

    def toBTableSchema: BTableSchema = BTableSchema(columns = columns.map(_.toBColumnDesc))
  }

  /**
   * The wrapper class of the Proto buffer `BColumnDesc`.
   */
  final case class ColumnDesc(
    private val columnName: String,
    private val typeDesc: TypeId
  ) extends Model {

    def toBColumnDesc: BColumnDesc =
      BColumnDesc(typeDesc = TypeId.toBOperationType(typeDesc), columnName = columnName)
  }

  /**
   * The wrapper class of the Proto buffer `BTypeId`.
   */
  object TypeId extends Enumeration {

    // define types <=> java.sql.Types in driver
    type TypeId = Value
    val B_TYPE_ID_UNSPECIFIED: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_UNSPECIFIED.index,
        BTypeId.B_TYPE_ID_UNSPECIFIED.name
      )
    val B_TYPE_ID_STRING_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_STRING_TYPE.index,
        BTypeId.B_TYPE_ID_STRING_TYPE.name
      )
    val B_TYPE_ID_INT_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_INT_TYPE.index,
        BTypeId.B_TYPE_ID_INT_TYPE.name
      )
    val B_TYPE_ID_DOUBLE_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_DOUBLE_TYPE.index,
        BTypeId.B_TYPE_ID_DOUBLE_TYPE.name
      )
    val B_TYPE_ID_LONG_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_LONG_TYPE.index,
        BTypeId.B_TYPE_ID_LONG_TYPE.name
      )
    val B_TYPE_ID_BOOLEAN_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_BOOLEAN_TYPE.index,
        BTypeId.B_TYPE_ID_BOOLEAN_TYPE.name
      )
    val B_TYPE_ID_TIMESTAMP_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_TIMESTAMP_TYPE.index,
        BTypeId.B_TYPE_ID_TIMESTAMP_TYPE.name
      )
    val B_TYPE_ID_SHORT_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_SHORT_TYPE.index,
        BTypeId.B_TYPE_ID_SHORT_TYPE.name
      )

    val B_TYPE_ID_BYTE_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_BYTE_TYPE.index,
        BTypeId.B_TYPE_ID_BYTE_TYPE.name
      )

    def toBOperationType(typeId: TypeId): BTypeId =
      BTypeId.fromValue(typeId.id)
  }

}
